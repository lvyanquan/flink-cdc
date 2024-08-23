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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.junit5.rds.AliyunRdsExtension;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Integration test for {@link MySqlSource} with RDS MySQL instance. */
class AliyunRdsBasedITCase {

    @RegisterExtension private static final AliyunRdsExtension RDS = new AliyunRdsExtension();

    @Test
    void testReadFromRdsMySql() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<RowData> source =
                createSourceBuilder(RDS.getCustomerTable().getDeserializer()).build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RDS MySQL Source");
        try (CloseableIterator<RowData> iterator = stream.executeAndCollect()) {
            List<String> records =
                    fetchRecords(
                            iterator,
                            RDS.getExpectedCustomerTableRecords().size(),
                            RDS.getCustomerTable()::stringify,
                            Duration.ofSeconds(30));
            assertThat(records).hasSameElementsAs(RDS.getExpectedCustomerTableRecords());
        }
    }

    @Test
    void testReadFromEarliestArchivedBinlog() throws Exception {
        long stopTimestampMs = System.currentTimeMillis();
        long startTimestampMs = 0;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> archivedBinlogFiles =
                RDS.getArchivedBinlogFiles(startTimestampMs, stopTimestampMs);
        assumeThat(archivedBinlogFiles).hasSizeGreaterThan(1);
        BinlogOffset startingOffset =
                BinlogOffset.ofBinlogFilePosition(archivedBinlogFiles.get(1), 4);
        MySqlSource<BinlogOffset> source =
                createSourceBuilder(binlogOffsetDeserializer())
                        .startupOptions(StartupOptions.specificOffset(startingOffset))
                        .build();
        // Note that "currentBinlogOffset" returns the offset of the NEXT event. We need to create
        // some binlog events to push the offset forward in order to make sure the loop can finish.
        BinlogOffset latestOnlineOffset =
                DebeziumUtils.currentBinlogOffset(RDS.getMySqlConnection());
        RDS.clearCustomerTable();
        // Unfortunately we can't validate contents in the archived binlog files. We just check if
        // events are monotonically increasing.
        DataStreamSource<BinlogOffset> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RDS MySQL Source");
        try (CloseableIterator<BinlogOffset> iterator = stream.executeAndCollect()) {
            BinlogOffset lastOffset = null;
            while (lastOffset == null
                    || compareBinlogFileOffset(lastOffset, latestOnlineOffset) < 0) {
                BinlogOffset recordOffset = iterator.next();
                assertThat(compareBinlogFileOffset(recordOffset, startingOffset))
                        .as("Records before starting offset should be dropped")
                        .isGreaterThanOrEqualTo(0);
                if (lastOffset != null) {
                    assertThat(compareBinlogFileOffset(recordOffset, lastOffset))
                            .as("Binlog offset should be monotonically increasing")
                            .isGreaterThanOrEqualTo(0);
                }
                lastOffset = recordOffset;
            }
        }
    }

    // --------------------------- Helper functions ----------------------------

    private static <T> MySqlSourceBuilder<T> createSourceBuilder(
            DebeziumDeserializationSchema<T> deserializer) {
        return MySqlSource.<T>builder()
                .hostname(RDS.getJdbcConnectionConfig().getHost())
                .port(RDS.getJdbcConnectionConfig().getPort())
                .username(RDS.getJdbcConnectionConfig().getUsername())
                .password(RDS.getJdbcConnectionConfig().getPassword())
                .databaseList(RDS.getJdbcConnectionConfig().getDatabase())
                .tableList(RDS.getCustomerTable().getTableId())
                .deserializer(deserializer)
                .enableReadingRdsArchivedBinlog(RDS.getRdsConfigBuilder().build());
    }

    private <T> List<String> fetchRecords(
            Iterator<T> iterator,
            int expectedSize,
            Function<T, String> stringifier,
            Duration timeout)
            throws Exception {
        List<String> records = new ArrayList<>();
        CompletableFuture<Void> future =
                new Watchdog()
                        .watch(
                                () -> {
                                    while (records.size() < expectedSize && iterator.hasNext()) {
                                        T record = iterator.next();
                                        records.add(stringifier.apply(record));
                                    }
                                },
                                () ->
                                        String.format(
                                                "Timeout getting expected number of records. \n"
                                                        + "Expected: %d record(s)\n"
                                                        + "Actual: %d record(s)\n"
                                                        + "%s",
                                                expectedSize, records.size(), records),
                                timeout);
        future.get();
        assertThat(records).hasSize(expectedSize);
        return records;
    }

    private BinlogOffsetDeserializer binlogOffsetDeserializer() {
        return new BinlogOffsetDeserializer();
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

    // -------------------------- Helper classes ------------------------

    private static class BinlogOffsetDeserializer
            implements DebeziumDeserializationSchema<BinlogOffset> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<BinlogOffset> out) {
            out.collect(RecordUtils.getBinlogPosition(record));
        }

        @Override
        public TypeInformation<BinlogOffset> getProducedType() {
            return TypeInformation.of(BinlogOffset.class);
        }
    }

    private static class Watchdog {
        private final Timer timer = new Timer();
        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        public CompletableFuture<Void> watch(
                Runnable action, Supplier<String> timeoutMessage, Duration timeout) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            executor.execute(
                    () -> {
                        action.run();
                        timer.cancel();
                        future.complete(null);
                    });
            timer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            future.completeExceptionally(
                                    new TimeoutException(timeoutMessage.get()));
                            executor.shutdown();
                        }
                    },
                    timeout.toMillis());
            return future;
        }
    }
}
