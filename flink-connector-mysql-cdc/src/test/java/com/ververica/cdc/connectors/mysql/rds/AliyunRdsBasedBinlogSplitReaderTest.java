/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.rds;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.binlog.LocalBinlogFile;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import com.ververica.cdc.connectors.mysql.testutils.junit5.rds.AliyunRdsExtension;
import io.debezium.connector.mysql.MySqlPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.discoverSchemaForCapturedTables;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link BinlogSplitReader} with RDS MySQL instance. */
class AliyunRdsBasedBinlogSplitReaderTest {
    private static final int SUBTASK_ID = 0;

    @RegisterExtension private static final AliyunRdsExtension RDS = new AliyunRdsExtension();

    @RegisterExtension
    private static final AliyunRdsExtension RDS_PARALLEL_ENABLED = new AliyunRdsExtension(true);

    static Stream<AliyunRdsExtension> testAliyunRdsExtensionProvider() {
        return Stream.of(RDS, RDS_PARALLEL_ENABLED);
    }

    @ParameterizedTest
    @MethodSource({"testAliyunRdsExtensionProvider"})
    void testReadFromArchivedOffsetWithSpecificTimestamp(AliyunRdsExtension aliyunRdsExtension)
            throws Exception {
        // Start reading from the earliest archived offset in 1 day
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        BinlogOffset earliestArchivedOffset =
                getFirstEventOffsetInArchiveWithSpecificTimestamp(
                        aliyunRdsExtension, startTimestampMs, stopTimestampMs);

        // Prepare reader
        BinlogSplitReader reader =
                new BinlogSplitReader(createStatefulTaskContext(aliyunRdsExtension), SUBTASK_ID);
        reader.submitSplit(createSplit(aliyunRdsExtension, earliestArchivedOffset));

        // Check if the binlog offset is monotonically increasing.
        // Unfortunately we can't validate record contents exactly, as we can't accurately archive
        // RDS binlog files at a specific offset :-(

        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(aliyunRdsExtension.getMySqlConnection());
        aliyunRdsExtension.clearCustomerTable();
        aliyunRdsExtension.insertIntoCustomerTable();

        BinlogOffset lastOffset = null;
        while (lastOffset == null || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
            Iterator<SourceRecords> iterator = reader.pollSplitRecords();
            while (iterator != null && iterator.hasNext()) {
                SourceRecords records = iterator.next();
                for (SourceRecord record : records.getSourceRecordList()) {
                    if (RecordUtils.isDataChangeRecord(record)) {
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
    }

    @ParameterizedTest
    @MethodSource({"testAliyunRdsExtensionProvider"})
    void testReadFromArchivedOffsetWithSpecificTimestampOfLastArchivedFile(
            AliyunRdsExtension aliyunRdsExtension) throws Exception {
        // Start reading from the earliest archived offset in 1 day
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        BinlogOffset earliestArchivedOffset =
                getLastEventOffsetInArchiveWithSpecificTimestamp(
                        aliyunRdsExtension, startTimestampMs, stopTimestampMs);

        // Prepare reader
        BinlogSplitReader reader =
                new BinlogSplitReader(createStatefulTaskContext(aliyunRdsExtension), SUBTASK_ID);
        reader.submitSplit(createSplit(aliyunRdsExtension, earliestArchivedOffset));

        // Check if the binlog offset is monotonically increasing.
        // Unfortunately we can't validate record contents exactly, as we can't accurately archive
        // RDS binlog files at a specific offset :-(

        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(aliyunRdsExtension.getMySqlConnection());
        aliyunRdsExtension.clearCustomerTable();
        aliyunRdsExtension.insertIntoCustomerTable();

        BinlogOffset lastOffset = null;
        while (lastOffset == null || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
            Iterator<SourceRecords> iterator = reader.pollSplitRecords();
            while (iterator != null && iterator.hasNext()) {
                SourceRecords records = iterator.next();
                for (SourceRecord record : records.getSourceRecordList()) {
                    if (RecordUtils.isDataChangeRecord(record)) {
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
    }

    @ParameterizedTest
    @MethodSource({"testAliyunRdsExtensionProvider"})
    void testReadFromArchivedOffsetWithSpecificFile(AliyunRdsExtension aliyunRdsExtension)
            throws Exception {
        // Start reading from the earliest archived offset in 1 day
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = stopTimestampMs - Duration.ofDays(1).toMillis();
        BinlogOffset earliestArchivedOffset =
                getFirstEventOffsetInArchiveWithSpecificFile(
                        aliyunRdsExtension, startTimestampMs, stopTimestampMs);

        // Prepare reader
        BinlogSplitReader reader =
                new BinlogSplitReader(createStatefulTaskContext(aliyunRdsExtension), SUBTASK_ID);
        reader.submitSplit(createSplit(aliyunRdsExtension, earliestArchivedOffset));

        // Check if the binlog offset is monotonically increasing.
        // Unfortunately we can't validate record contents exactly, as we can't accurately archive
        // RDS binlog files at a specific offset :-(

        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(aliyunRdsExtension.getMySqlConnection());
        aliyunRdsExtension.clearCustomerTable();

        BinlogOffset lastOffset = null;
        while (lastOffset == null || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
            Iterator<SourceRecords> iterator = reader.pollSplitRecords();
            while (iterator != null && iterator.hasNext()) {
                SourceRecords records = iterator.next();
                for (SourceRecord record : records.getSourceRecordList()) {
                    if (RecordUtils.isDataChangeRecord(record)) {
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
    }

    private MySqlBinlogSplit createSplit(
            AliyunRdsExtension aliyunRdsExtension, BinlogOffset startingOffset) {
        MySqlSourceConfig sourceConfig =
                aliyunRdsExtension.getSourceConfigFactory().createConfig(SUBTASK_ID);
        return new MySqlBinlogSplit(
                "foo-split",
                startingOffset,
                BinlogOffset.ofNonStopping(),
                Collections.emptyList(),
                discoverSchemaForCapturedTables(
                        new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                        sourceConfig,
                        aliyunRdsExtension.getMySqlConnection()),
                0);
    }

    private BinlogOffset getFirstEventOffsetInArchiveWithSpecificTimestamp(
            AliyunRdsExtension aliyunRdsExtension, long startTimestampMs, long stopTimestampMs)
            throws Exception {
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(aliyunRdsExtension, startTimestampMs, stopTimestampMs)) {
            LocalBinlogFile firstFile = fetcher.next();
            assertThat(firstFile).isNotNull();
            try (BinaryLogFileReader binlogFileReader = createBinlogFileReader(firstFile)) {
                Event firstEvent = binlogFileReader.readEvent();
                return BinlogOffset.ofTimestampSec(firstEvent.getHeader().getTimestamp() / 1000);
            }
        }
    }

    private BinlogOffset getLastEventOffsetInArchiveWithSpecificTimestamp(
            AliyunRdsExtension aliyunRdsExtension, long startTimestampMs, long stopTimestampMs)
            throws Exception {
        BinlogOffset lastEventOffset = null;
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(aliyunRdsExtension, startTimestampMs, stopTimestampMs)) {
            while (fetcher.hasNext()) {
                LocalBinlogFile localBinlogFile = fetcher.next();
                assertThat(localBinlogFile).isNotNull();
                try (BinaryLogFileReader binlogFileReader =
                        createBinlogFileReader(localBinlogFile)) {
                    Event firstEvent = binlogFileReader.readEvent();
                    lastEventOffset =
                            BinlogOffset.ofTimestampSec(
                                    firstEvent.getHeader().getTimestamp() / 1000);
                }
            }
        }
        return lastEventOffset;
    }

    private BinlogOffset getFirstEventOffsetInArchiveWithSpecificFile(
            AliyunRdsExtension aliyunRdsExtension, long startTimestampMs, long stopTimestampMs)
            throws Exception {
        try (AliyunRdsBinlogFileFetcher fetcher =
                createFetcher(aliyunRdsExtension, startTimestampMs, stopTimestampMs)) {
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

    private StatefulTaskContext createStatefulTaskContext(AliyunRdsExtension aliyunRdsExtension) {
        MySqlSourceConfig sourceConfig =
                aliyunRdsExtension.getSourceConfigFactory().createConfig(SUBTASK_ID);
        return new StatefulTaskContext(
                sourceConfig,
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration()),
                aliyunRdsExtension.getMySqlConnection(),
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
    }

    private AliyunRdsBinlogFileFetcher createFetcher(
            AliyunRdsExtension aliyunRdsExtension, long startTimestampMs, long stopTimestampMs) {
        AliyunRdsBinlogFileFetcher fetcher =
                new AliyunRdsBinlogFileFetcher(
                        aliyunRdsExtension.getRdsConfigBuilder().build(),
                        startTimestampMs,
                        stopTimestampMs);
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
