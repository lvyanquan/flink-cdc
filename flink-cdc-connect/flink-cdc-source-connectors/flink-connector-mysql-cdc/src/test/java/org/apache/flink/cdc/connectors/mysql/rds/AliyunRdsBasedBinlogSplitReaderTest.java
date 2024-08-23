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

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.LocalBinlogFile;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.junit5.rds.AliyunRdsExtension;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import io.debezium.connector.mysql.MySqlPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.discoverSchemaForCapturedTables;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link BinlogSplitReader} with RDS MySQL instance. */
class AliyunRdsBasedBinlogSplitReaderTest {
    private static final int SUBTASK_ID = 0;
    @RegisterExtension private static final AliyunRdsExtension RDS = new AliyunRdsExtension();

    @Test
    void testReadFromArchivedOffsetWithSpecificTimestamp() throws Exception {
        // Start reading from the earliest archived offset in 1 day
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        BinlogOffset earliestArchivedOffset =
                getFirstEventOffsetInArchiveWithSpecificTimestamp(
                        startTimestampMs, stopTimestampMs);
        // Prepare reader
        BinlogSplitReader reader = new BinlogSplitReader(createStatefulTaskContext(), SUBTASK_ID);
        reader.submitSplit(createSplit(earliestArchivedOffset));
        // Check if the binlog offset is monotonically increasing.
        // Unfortunately we can't validate record contents exactly, as we can't accurately archive
        // RDS binlog files at a specific offset :-(
        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(RDS.getMySqlConnection());
        RDS.clearCustomerTable();
        BinlogOffset lastOffset = null;
        while (lastOffset == null || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
            Iterator<SourceRecords> iterator = reader.pollSplitRecords();
            while (iterator != null && iterator.hasNext()) {
                SourceRecords records = iterator.next();
                for (SourceRecord record : records.getSourceRecordList()) {
                    BinlogOffset currentOffset = RecordUtils.getBinlogPosition(record);
                    if (lastOffset != null) {
                        assertThat(compareBinlogFileOffset(currentOffset, lastOffset))
                                .isGreaterThanOrEqualTo(0);
                    }
                    lastOffset = currentOffset;
                }
            }
        }
    }

    @Test
    void testReadFromArchivedOffsetWithSpecificFile() throws Exception {
        // Start reading from the earliest archived offset in 1 day
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        BinlogOffset earliestArchivedOffset =
                getFirstEventOffsetInArchiveWithSpecificFile(startTimestampMs, stopTimestampMs);
        // Prepare reader
        BinlogSplitReader reader = new BinlogSplitReader(createStatefulTaskContext(), SUBTASK_ID);
        reader.submitSplit(createSplit(earliestArchivedOffset));
        // Check if the binlog offset is monotonically increasing.
        // Unfortunately we can't validate record contents exactly, as we can't accurately archive
        // RDS binlog files at a specific offset :-(
        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(RDS.getMySqlConnection());
        RDS.clearCustomerTable();
        BinlogOffset lastOffset = null;
        while (lastOffset == null || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
            Iterator<SourceRecords> iterator = reader.pollSplitRecords();
            while (iterator != null && iterator.hasNext()) {
                SourceRecords records = iterator.next();
                for (SourceRecord record : records.getSourceRecordList()) {
                    BinlogOffset currentOffset = RecordUtils.getBinlogPosition(record);
                    if (lastOffset != null) {
                        assertThat(compareBinlogFileOffset(currentOffset, lastOffset))
                                .isGreaterThanOrEqualTo(0);
                    }
                    lastOffset = currentOffset;
                }
            }
        }
    }

    private MySqlBinlogSplit createSplit(BinlogOffset startingOffset) {
        MySqlSourceConfig sourceConfig = RDS.getSourceConfigFactory().createConfig(SUBTASK_ID);
        return new MySqlBinlogSplit(
                "foo-split",
                startingOffset,
                BinlogOffset.ofNonStopping(),
                Collections.emptyList(),
                discoverSchemaForCapturedTables(
                        new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                        RDS.getSourceConfigFactory().createConfig(SUBTASK_ID),
                        RDS.getMySqlConnection()),
                0);
    }

    private BinlogOffset getFirstEventOffsetInArchiveWithSpecificTimestamp(
            long startTimestampMs, long stopTimestampMs) throws Exception {
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(startTimestampMs, stopTimestampMs)) {
            LocalBinlogFile firstFile = fetcher.next();
            assertThat(firstFile).isNotNull();
            try (BinaryLogFileReader binlogFileReader = createBinlogFileReader(firstFile)) {
                Event firstEvent = binlogFileReader.readEvent();
                return BinlogOffset.ofTimestampSec(firstEvent.getHeader().getTimestamp() / 1000);
            }
        }
    }

    private BinlogOffset getFirstEventOffsetInArchiveWithSpecificFile(
            long startTimestampMs, long stopTimestampMs) throws Exception {
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(startTimestampMs, stopTimestampMs)) {
            LocalBinlogFile firstFile = fetcher.next();
            assertThat(firstFile).isNotNull();
            try (BinaryLogFileReader binlogFileReader = createBinlogFileReader(firstFile)) {
                Event firstEvent = binlogFileReader.readEvent();
                return BinlogOffset.builder()
                        .setBinlogFilePosition(
                                firstFile.getFilename(),
                                ((EventHeaderV4) firstEvent.getHeader()).getPosition())
                        .setTimestampSec(firstEvent.getHeader().getTimestamp() / 1000)
                        .build();
            }
        }
    }

    private StatefulTaskContext createStatefulTaskContext() {
        MySqlSourceConfig sourceConfig = RDS.getSourceConfigFactory().createConfig(SUBTASK_ID);
        return new StatefulTaskContext(
                sourceConfig,
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration()),
                RDS.getMySqlConnection(),
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
    }

    private AliyunRdsBinlogFileFetcher createFetcher(long startTimestampMs, long stopTimestampMs) {
        AliyunRdsBinlogFileFetcher fetcher =
                new AliyunRdsBinlogFileFetcher(
                        RDS.getRdsConfigBuilder().build(), startTimestampMs, stopTimestampMs);
        fetcher.persistDownloadedFiles();
        fetcher.initialize(BinlogOffset.ofBinlogFilePosition("mysql-bin.000001", 4));
        return fetcher;
    }

    private BinaryLogFileReader createBinlogFileReader(LocalBinlogFile binlogFile)
            throws IOException {
        return new BinaryLogFileReader(Files.newInputStream(binlogFile.getPath()));
    }

    private int compareBinlogFileOffset(BinlogOffset a, BinlogOffset b) {
        if (a.getFilename().compareToIgnoreCase(b.getFilename()) != 0) {
            return a.getFilename().compareToIgnoreCase(b.getFilename());
        }
        if (a.getPosition() != b.getPosition()) {
            return Long.compare(a.getPosition(), b.getPosition());
        }
        return 0;
    }
}
