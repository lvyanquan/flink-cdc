/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.rds;

import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.LocalBinlogFile;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.testutils.junit5.rds.AliyunRdsExtension;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link AliyunRdsBinlogFileFetcher}. */
class AliyunRdsBinlogFileFetcherTest {

    @RegisterExtension static final AliyunRdsExtension RDS = new AliyunRdsExtension();

    @Test
    void testGetAllRdsBinlogFiles() throws Exception {
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = 0;
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(RDS, startTimestampMs, stopTimestampMs)) {
            int numRdsBinlogFiles = fetcher.getRdsBinlogFileQueue().size();
            // Check that we have retrieved file information larger than one page.
            Assertions.assertTrue(numRdsBinlogFiles > 30);
        }
    }

    @Test
    void testFetchingBinlogFiles() throws Exception {
        // Get archived binlog files between T-48h and T-24h
        long stopTimestampMs = System.currentTimeMillis() - Duration.ofDays(1).toMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(RDS, startTimestampMs, stopTimestampMs)) {
            int numRdsBinlogFiles = fetcher.getRdsBinlogFileQueue().size();
            int numLocalBinlogFiles = 0;
            LocalBinlogFile lastFile = null;
            while (fetcher.hasNext()) {
                LocalBinlogFile localBinlogFile = fetcher.next();
                assertThat(localBinlogFile).isNotNull();
                try (BinaryLogFileReader reader = createBinlogFileReader(localBinlogFile)) {
                    FirstAndLastEvent firstAndLastEvent = readFirstAndLastEvent(reader);
                    assertThat(firstAndLastEvent.first.getHeader().getTimestamp())
                            .as(
                                    "Timestamp of the first event in the binlog file "
                                            + "should be less than the stopping timestamp")
                            .isLessThan(stopTimestampMs);
                    assertThat(firstAndLastEvent.last.getHeader().getTimestamp())
                            .as(
                                    "Timestamp of the last event in the binlog file "
                                            + "should be greater than the starting timestamp")
                            .isGreaterThan(startTimestampMs);
                }
                if (lastFile != null) {
                    assertThat(localBinlogFile)
                            .as("Check the order of binlog files")
                            .isGreaterThan(lastFile);
                }
                lastFile = localBinlogFile;
                ++numLocalBinlogFiles;
            }
            assertThat(numLocalBinlogFiles).isEqualTo(numRdsBinlogFiles);
        }
    }

    @Test
    void testNoBinlogFiles() throws Exception {
        // Use the same value for both start and stop timestamp
        long timestamp = System.currentTimeMillis();
        try (AliyunRdsBinlogFileFetcher fetcher = createFetcher(RDS, timestamp, timestamp)) {
            assertThat(fetcher.getRdsBinlogFileQueue()).isEmpty();
            assertThat(fetcher.hasNext()).isFalse();
        }
    }

    @Test
    void testDeleteBinlogFileAfterServing() throws Exception {
        // Get archived binlog files between T-48h and T-24h
        long stopTimestampMs = System.currentTimeMillis() - Duration.ofDays(1).toMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(RDS, startTimestampMs, stopTimestampMs)) {
            LocalBinlogFile lastFile = null;
            while (fetcher.hasNext()) {
                LocalBinlogFile file = fetcher.next();
                if (lastFile != null) {
                    assertThat(lastFile.getPath()).doesNotExist();
                }
                lastFile = file;
            }
            assertThat(fetcher.getBinlogFileDirectory()).isEmptyDirectory();
        }
    }

    private AliyunRdsBinlogFileFetcher createFetcher(
            AliyunRdsExtension aliyunRdsExtension, long startTimestampMs, long stopTimestampMs) {
        AliyunRdsBinlogFileFetcher fetcher =
                new AliyunRdsBinlogFileFetcher(
                        aliyunRdsExtension.getRdsConfigBuilder().build(),
                        startTimestampMs,
                        stopTimestampMs);
        fetcher.initialize(BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4));
        return fetcher;
    }

    private BinaryLogFileReader createBinlogFileReader(LocalBinlogFile binlogFile)
            throws Exception {
        return new BinaryLogFileReader(Files.newInputStream(binlogFile.getPath()));
    }

    private FirstAndLastEvent readFirstAndLastEvent(BinaryLogFileReader reader) throws Exception {
        Event first = reader.readEvent();
        Event last = first;
        Event current;
        while ((current = reader.readEvent()) != null) {
            last = current;
        }
        return new FirstAndLastEvent(first, last);
    }

    private static class FirstAndLastEvent {
        private final Event first;
        private final Event last;

        public FirstAndLastEvent(Event first, Event last) {
            this.first = first;
            this.last = last;
        }
    }
}
