/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.types.RowKind;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.reader.MySqlRecordEmitter;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import com.ververica.cdc.connectors.mysql.table.MySqlTableSpec;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.ververica.cdc.connectors.mysql.testutils.MetricsUtils.getMySqlSplitEnumeratorContext;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MySqlEvolvingSourceDeserializeSchema}. */
@RunWith(Parameterized.class)
public class MySqlEvolvingSourceDeserializeSchemaTest extends MySqlSourceTestBase {

    private final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final MySqlSourceConfig sourceConfig =
            getConfig(customerDatabase, new String[] {"customers"}, StartupOptions.earliest());
    private final ObjectPath tablePathInFlink = new ObjectPath("customer", "customers");

    private TableId tablePathInDB;
    private MySqlBinlogSplit binlogSplit;

    private BinaryLogClient binaryLogClient;
    private MySqlConnection mySqlConnection;

    @Before
    public void setup() throws Exception {
        customerDatabase.createAndInitialize();
        binlogSplit = createBinlogSplit(sourceConfig);
        tablePathInDB = binlogSplit.getTableSchemas().keySet().iterator().next();
        binaryLogClient = DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        mySqlConnection = DebeziumUtils.createMySqlConnection(sourceConfig);
    }

    @Test
    public void testReadSchemaEvolution() throws Exception {
        makeSchemaChangeEvents(tablePathInDB.toString());

        BinlogSplitReader reader = createReader(sourceConfig);
        reader.submitSplit(binlogSplit);
        MySqlEvolvingSourceDeserializeSchema deserializeSchema =
                new MySqlEvolvingSourceDeserializeSchema(
                        Collections.singleton(
                                new MySqlTableSpec(
                                        tablePathInFlink,
                                        new ObjectPath(
                                                customerDatabase.getDatabaseName(), "customers"),
                                        false)),
                        ZoneId.of("UTC"),
                        ScanRuntimeProviderContext.INSTANCE.getEvolvingSourceTypeInfo(),
                        true,
                        false);
        for (TableChanges.TableChange change : binlogSplit.getTableSchemas().values()) {
            deserializeSchema.applyTableChange(change);
        }
        SimpleOutput output = new SimpleOutput();

        Thread workerThread =
                new Thread(
                        consumeRecords(
                                reader.pollSplitRecords(), deserializeSchema, binlogSplit, output));

        // Catch async exception in the fetcher thread
        AtomicReference<Throwable> asyncException = new AtomicReference<>();
        workerThread.setUncaughtExceptionHandler(
                (t, e) -> {
                    if (asyncException.get() == null) {
                        asyncException.set(e);
                    } else {
                        asyncException.get().addSuppressed(e);
                    }
                });
        workerThread.start();

        validateOutput(output, asyncException, getExpectedRecords(), Duration.ofSeconds(30));
    }

    private BinlogSplitReader createReader(MySqlSourceConfig configuration) {
        final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(
                        configuration,
                        binaryLogClient,
                        mySqlConnection,
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricsGroup.createSourceReaderMetricGroup()));
        return new BinlogSplitReader(statefulTaskContext, 0);
    }

    private MySqlBinlogSplit createBinlogSplit(MySqlSourceConfig configuration) throws Exception {
        MySqlBinlogSplitAssigner binlogSplitAssigner =
                new MySqlBinlogSplitAssigner(configuration, getMySqlSplitEnumeratorContext());
        binlogSplitAssigner.open();
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            Map<TableId, TableChanges.TableChange> tableSchemas =
                    TableDiscoveryUtils.discoverSchemaForCapturedTables(
                            new MySqlPartition(
                                    sourceConfig.getMySqlConnectorConfig().getLogicalName()),
                            sourceConfig,
                            jdbc);
            return MySqlBinlogSplit.fillTableSchemas(
                    binlogSplitAssigner.getNext().get().asBinlogSplit(), tableSchemas);
        }
    }

    private Runnable consumeRecords(
            Iterator<SourceRecords> sourceRecordsIterator,
            MySqlEvolvingSourceDeserializeSchema deserializeSchema,
            MySqlBinlogSplit split,
            SourceOutput<SourceRecord> output) {
        MySqlRecordEmitter<SourceRecord> recordEmitter =
                new MySqlRecordEmitter<>(
                        deserializeSchema,
                        new MySqlSourceReaderMetrics(new TestingReaderContext().metricGroup()),
                        sourceConfig.isIncludeSchemaChanges());
        MySqlSplitState splitState = new MySqlBinlogSplitState(split);
        return () -> {
            while (sourceRecordsIterator.hasNext()) {
                try {
                    recordEmitter.emitRecord(sourceRecordsIterator.next(), output, splitState);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to emit records.", e);
                }
            }
        };
    }

    private void makeSchemaChangeEvents(String tableId) throws SQLException {
        mySqlConnection.setAutoCommit(false);
        // ALTER TABLE
        mySqlConnection.execute(
                String.format("ALTER TABLE %s ADD COLUMN description VARCHAR(255)", tableId));
        mySqlConnection.execute(
                String.format(
                        "INSERT INTO %s VALUES (210, 'user_210', 'Ningbo', 15957199698, 'Worker')",
                        tableId));
        mySqlConnection.execute(String.format("DELETE FROM %s WHERE id = 1012", tableId));
        mySqlConnection.execute(
                String.format("UPDATE %s SET address = 'Hangzhou' WHERE id = 101", tableId));
        mySqlConnection.commit();
        // DROP TABLE
        mySqlConnection.execute(String.format("DROP TABLE %s", tableId));
        mySqlConnection.commit();
        // CREATE TABLE
        mySqlConnection.execute(
                String.format("CREATE TABLE %s(id INT, name VARCHAR(255))", tableId),
                String.format("INSERT INTO %s VALUES (211, 'user_211')", tableId));
        mySqlConnection.commit();
    }

    private void validateOutput(
            SimpleOutput output,
            AtomicReference<Throwable> exception,
            List<SourceRecord> expectedRecords,
            Duration timeout)
            throws InterruptedException {
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            if (exception.get() != null) {
                throw new RuntimeException("Uncaught async exception", exception.get());
            }
            if (output.bufferedRecords.size() < expectedRecords.size()) {
                Thread.sleep(100);
            } else {
                break;
            }
        }

        List<SourceRecord> actual = new ArrayList<>(output.bufferedRecords);

        assertThat(actual).containsAll(expectedRecords);
    }

    private List<SourceRecord> getExpectedRecords() {
        SchemaSpec schemaAfterAddColumn =
                SchemaSpec.fromRowDataType(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("name", DataTypes.VARCHAR(255).notNull()),
                                DataTypes.FIELD("address", DataTypes.VARCHAR(1024)),
                                DataTypes.FIELD("phone_number", DataTypes.VARCHAR(512)),
                                DataTypes.FIELD("description", DataTypes.VARCHAR(255))));
        // TODO: after drop table the cdc source doesn't get binlogs about delete rows in the
        // dropped table
        // We may need to add a special record to tell the {@code SchemaChangeListener} to drop the
        // table.
        return Arrays.asList(
                // INSERT statement
                new SourceRecord(
                        tablePathInFlink,
                        schemaAfterAddColumn,
                        new JoinedRowData(
                                RowKind.INSERT,
                                GenericRowData.ofKind(
                                        RowKind.INSERT,
                                        210,
                                        BinaryStringData.fromString("user_210"),
                                        BinaryStringData.fromString("Ningbo"),
                                        BinaryStringData.fromString("15957199698"),
                                        BinaryStringData.fromString("Worker")),
                                new GenericRowData(0))),
                // DELETE statement
                new SourceRecord(
                        tablePathInFlink,
                        schemaAfterAddColumn,
                        new JoinedRowData(
                                RowKind.DELETE,
                                GenericRowData.ofKind(
                                        RowKind.DELETE,
                                        1012,
                                        BinaryStringData.fromString("user_13"),
                                        BinaryStringData.fromString("Shanghai"),
                                        BinaryStringData.fromString("123567891234"),
                                        null),
                                new GenericRowData(0))),
                // UPDATE statement
                new SourceRecord(
                        tablePathInFlink,
                        schemaAfterAddColumn,
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        101,
                                        BinaryStringData.fromString("user_1"),
                                        BinaryStringData.fromString("Shanghai"),
                                        BinaryStringData.fromString("123567891234"),
                                        null),
                                new GenericRowData(0))),
                new SourceRecord(
                        tablePathInFlink,
                        schemaAfterAddColumn,
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        101,
                                        BinaryStringData.fromString("user_1"),
                                        BinaryStringData.fromString("Hangzhou"),
                                        BinaryStringData.fromString("123567891234"),
                                        null),
                                new GenericRowData(0))),
                // INSERT statement
                new SourceRecord(
                        tablePathInFlink,
                        SchemaSpec.fromRowDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.VARCHAR(255)))),
                        new JoinedRowData(
                                RowKind.INSERT,
                                GenericRowData.ofKind(
                                        RowKind.INSERT,
                                        211,
                                        BinaryStringData.fromString("user_211")),
                                new GenericRowData(0))));
    }

    private static class SimpleOutput implements SourceOutput<SourceRecord> {

        private final List<SourceRecord> bufferedRecords;

        public SimpleOutput() {
            bufferedRecords = Collections.synchronizedList(new ArrayList<>());
        }

        @Override
        public void collect(SourceRecord record) {
            bufferedRecords.add(record);
        }

        @Override
        public void collect(SourceRecord record, long l) {
            bufferedRecords.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void markActive() {}
    }
}
