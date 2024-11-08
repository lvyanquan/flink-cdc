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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommitRequestImpl;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.options.Options;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypes.STRING;

/** An ITCase for {@link PaimonWriter} and {@link PaimonCommitter}. */
public class PaimonSinkITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private Options catalogOptions;

    private TableEnvironment tEnv;

    private String warehouse;

    private final TableId table1 = TableId.tableId("test", "table1");
    private final TableId table2 = TableId.tableId("test", "table2");

    public static final String TEST_DATABASE = "test";
    private static final String HADOOP_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hadoop-conf-dir"))
                    .getPath();

    private static final String HIVE_CONF_DIR =
            Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader()
                                    .getResource("hive-conf-dir"))
                    .getPath();

    private void initialize(String metastore)
            throws Catalog.DatabaseNotEmptyException, Catalog.DatabaseNotExistException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions = new Options();
        catalogOptions.setString("metastore", metastore);
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        String catalogName = "paimon_catalog";
        tEnv.registerCatalog(
                catalogName,
                new FlinkCatalog(
                        catalog,
                        catalogName,
                        "test",
                        this.getClass().getClassLoader(),
                        catalogOptions));

        catalog.dropDatabase(TEST_DATABASE, true, true);
    }

    private List<Event> createTestEvents() throws SchemaEvolveException {
        List<Event> testEvents = new ArrayList<>();
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", STRING())
                        .physicalColumn("col2", STRING())
                        .primaryKey("col1")
                        .option("bucket", "1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(createTableEvent);

        // insert
        testEvents.add(generateInsert(table1, Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1")));
        testEvents.add(generateInsert(table1, Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "2")));
        return testEvents;
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testSinkWithDataChange(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        Assumptions.assumeFalse(
                metastore.equals("hive"), "Hive metastore does not work with DLF now.");

        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions,
                        new PaimonRecordEventSerializer(ZoneId.systemDefault()),
                        null);
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // insert
        writeAndCommit(writer, committer, createTestEvents().toArray(new Event[0]));
        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, "1", "1"),
                                Row.ofKind(RowKind.INSERT, "2", "2")));

        // delete
        writeAndCommit(
                writer,
                committer,
                generateDelete(table1, Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1")));

        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, "2", "2")));

        // update
        writeAndCommit(
                writer,
                committer,
                generateUpdate(
                        table1,
                        Arrays.asList(Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "2")),
                        Arrays.asList(Tuple2.of(STRING(), "2"), Tuple2.of(STRING(), "x"))));
        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, "2", "x")));

        List<Row> result = new ArrayList<>();
        tEnv.sqlQuery("select max_sequence_number from paimon_catalog.test.`table1$files`")
                .execute()
                .collect()
                .forEachRemaining(result::add);
        // Each commit will generate one sequence number(equal to checkpointId).
        org.junit.jupiter.api.Assertions.assertEquals(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1L),
                        Row.ofKind(RowKind.INSERT, 2L),
                        Row.ofKind(RowKind.INSERT, 3L)),
                result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem", "hive"})
    public void testSinkWithSchemaChange(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        Assumptions.assumeFalse(
                metastore.equals("hive"), "Hive metastore does not work with DLF now.");

        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions,
                        new PaimonRecordEventSerializer(ZoneId.systemDefault()),
                        null);
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();

        // 1. receive only DataChangeEvents during one checkpoint
        writeAndCommit(writer, committer, createTestEvents().toArray(new Event[0]));
        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, "1", "1"),
                                Row.ofKind(RowKind.INSERT, "2", "2")));

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                writer,
                committer,
                generateInsert(table1, Tuple2.of(STRING(), "3"), Tuple2.of(STRING(), "3")));

        // add column
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(Column.physicalColumn("col3", STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        writer.write(addColumnEvent, null);
        metadataApplier.applySchemaChange(addColumnEvent);

        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1,
                        Tuple2.of(STRING(), "4"),
                        Tuple2.of(STRING(), "4"),
                        Tuple2.of(STRING(), "4")));

        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, "1", "1", null),
                                Row.ofKind(RowKind.INSERT, "2", "2", null),
                                Row.ofKind(RowKind.INSERT, "3", "3", null),
                                Row.ofKind(RowKind.INSERT, "4", "4", "4")));

        // 2. receive DataChangeEvents and SchemaChangeEvents during one checkpoint
        writeAndCommit(
                writer,
                committer,
                generateInsert(
                        table1,
                        Tuple2.of(STRING(), "5"),
                        Tuple2.of(STRING(), "5"),
                        Tuple2.of(STRING(), "5")));

        // drop column
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("col2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        writer.write(dropColumnEvent, null);

        writeAndCommit(
                writer,
                committer,
                generateInsert(table1, Tuple2.of(STRING(), "6"), Tuple2.of(STRING(), "6")));

        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, "1", null),
                                Row.ofKind(RowKind.INSERT, "2", null),
                                Row.ofKind(RowKind.INSERT, "3", null),
                                Row.ofKind(RowKind.INSERT, "4", "4"),
                                Row.ofKind(RowKind.INSERT, "5", "5"),
                                Row.ofKind(RowKind.INSERT, "6", "6")));

        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(table1);
        metadataApplier.applySchemaChange(truncateTableEvent);

        writeAndCommit(
                writer,
                committer,
                generateInsert(table1, Tuple2.of(STRING(), "7"), Tuple2.of(STRING(), "7")));

        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, "7", "7")));

        DropTableEvent dropTableEvent = new DropTableEvent(table1);
        metadataApplier.applySchemaChange(dropTableEvent);
        Assertions.assertThatThrownBy(() -> fetchResults(table1))
                .hasRootCauseExactlyInstanceOf(SqlValidatorException.class)
                .hasRootCauseMessage("Object 'table1' not found within 'paimon_catalog.test'");
    }

    @ParameterizedTest
    @ValueSource(strings = {"filesystem"})
    public void testSinkWithMultiTables(String metastore)
            throws IOException, InterruptedException, Catalog.DatabaseNotEmptyException,
                    Catalog.DatabaseNotExistException, SchemaEvolveException {
        initialize(metastore);
        PaimonSink<Event> paimonSink =
                new PaimonSink<>(
                        catalogOptions,
                        new PaimonRecordEventSerializer(ZoneId.systemDefault()),
                        null);
        PaimonWriter<Event> writer = paimonSink.createWriter(new MockInitContext());
        Committer<MultiTableCommittable> committer = paimonSink.createCommitter();
        List<Event> testEvents = createTestEvents();

        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", STRING())
                        .physicalColumn("col2", STRING())
                        .primaryKey("col1")
                        .option("bucket", "1")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table2, schema);
        testEvents.add(createTableEvent);
        PaimonMetadataApplier metadataApplier = new PaimonMetadataApplier(catalogOptions);
        metadataApplier.applySchemaChange(createTableEvent);
        // insert
        testEvents.add(generateInsert(table2, Tuple2.of(STRING(), "1"), Tuple2.of(STRING(), "1")));

        // insert
        writeAndCommit(writer, committer, testEvents.toArray(new Event[0]));

        Assertions.assertThat(fetchResults(table1))
                .isEqualTo(
                        Arrays.asList(
                                Row.ofKind(RowKind.INSERT, "1", "1"),
                                Row.ofKind(RowKind.INSERT, "2", "2")));
        Assertions.assertThat(fetchResults(table2))
                .isEqualTo(Collections.singletonList(Row.ofKind(RowKind.INSERT, "1", "1")));
    }

    private static void commit(
            PaimonWriter<Event> writer, Committer<MultiTableCommittable> committer)
            throws IOException, InterruptedException {
        Collection<Committer.CommitRequest<MultiTableCommittable>> commitRequests =
                writer.prepareCommit().stream()
                        .map(MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        committer.commit(commitRequests);
    }

    private static void writeAndCommit(
            PaimonWriter<Event> writer, Committer<MultiTableCommittable> committer, Event... events)
            throws IOException, InterruptedException {
        for (Event event : events) {
            writer.write(event, null);
        }
        writer.flush(false);
        commit(writer, committer);
    }

    private List<Row> fetchResults(TableId tableId) {
        List<Row> results = new ArrayList<>();
        tEnv.sqlQuery("select * from paimon_catalog." + tableId.toString())
                .execute()
                .collect()
                .forEachRemaining(results::add);
        return results;
    }

    private BinaryRecordData generate(Tuple2<DataType, Object>... elements) {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                Arrays.stream(elements).map(e -> e.f0).toArray(DataType[]::new)));
        return generator.generate(
                Arrays.stream(elements)
                        .map(e -> e.f1)
                        .map(o -> o instanceof String ? BinaryStringData.fromString((String) o) : o)
                        .toArray(Object[]::new));
    }

    private DataChangeEvent generateInsert(TableId tableId, Tuple2<DataType, Object>... elements) {
        return DataChangeEvent.insertEvent(tableId, generate(elements));
    }

    private DataChangeEvent generateUpdate(
            TableId tableId,
            List<Tuple2<DataType, Object>> beforeElements,
            List<Tuple2<DataType, Object>> afterElements) {
        return DataChangeEvent.updateEvent(
                tableId,
                generate(beforeElements.toArray(new Tuple2[0])),
                generate(afterElements.toArray(new Tuple2[0])));
    }

    private DataChangeEvent generateDelete(TableId tableId, Tuple2<DataType, Object>... elements) {
        return DataChangeEvent.deleteEvent(tableId, generate(elements));
    }

    private static class MockCommitRequestImpl<CommT> extends CommitRequestImpl<CommT> {

        protected MockCommitRequestImpl(CommT committable) {
            super(committable);
        }
    }

    private static class MockInitContext
            implements Sink.InitContext, SerializationSchema.InitializationContext {

        private MockInitContext() {}

        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        public ProcessingTimeService getProcessingTimeService() {
            return null;
        }

        public int getSubtaskId() {
            return 0;
        }

        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        public int getAttemptNumber() {
            return 0;
        }

        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        public MetricGroup getMetricGroup() {
            return null;
        }

        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }

        public boolean isObjectReuseEnabled() {
            return false;
        }

        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        public JobID getJobId() {
            return null;
        }
    }
}
