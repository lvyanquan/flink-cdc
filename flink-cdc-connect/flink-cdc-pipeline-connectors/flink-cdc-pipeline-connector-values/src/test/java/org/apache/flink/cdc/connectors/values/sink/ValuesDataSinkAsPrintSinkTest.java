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

package org.apache.flink.cdc.connectors.values.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.values.sink.ValuesDataSink.SinkApi.SINK_FUNCTION;
import static org.apache.flink.cdc.connectors.values.sink.ValuesDataSink.SinkApi.SINK_V2;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ValuesDataSink} when print = true. */
class ValuesDataSinkAsPrintSinkTest {

    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    private final ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayOutputStream arrayErrorStream = new ByteArrayOutputStream();

    private final String line = System.lineSeparator();

    private final TableId testTableId = TableId.tableId("testDB", "testTable");
    private final Schema testTableSchemaBefore =
            Schema.newBuilder()
                    .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("name", DataTypes.STRING()))
                    .column(Column.physicalColumn("age", DataTypes.INT()))
                    .build();
    private final Schema testTableSchemaAfter =
            Schema.newBuilder()
                    .column(Column.physicalColumn("id", DataTypes.BIGINT()))
                    .column(Column.physicalColumn("name", DataTypes.STRING()))
                    .column(Column.physicalColumn("age", DataTypes.INT()))
                    .column(Column.physicalColumn("gender", DataTypes.STRING()))
                    .build();
    private final BinaryRecordDataGenerator beforeDataGenerator =
            new BinaryRecordDataGenerator(
                    testTableSchemaBefore.getColumnDataTypes().toArray(new DataType[0]));
    private final BinaryRecordDataGenerator afterDataGenerator =
            new BinaryRecordDataGenerator(
                    testTableSchemaAfter.getColumnDataTypes().toArray(new DataType[0]));
    private final List<Event> testData =
            Arrays.asList(
                    new CreateTableEvent(testTableId, testTableSchemaBefore),
                    DataChangeEvent.insertEvent(
                            testTableId,
                            beforeDataGenerator.generate(
                                    new Object[] {1L, BinaryStringData.fromString("Alice"), 18})),
                    DataChangeEvent.insertEvent(
                            testTableId,
                            beforeDataGenerator.generate(
                                    new Object[] {2L, BinaryStringData.fromString("Charlie"), 28})),
                    new AddColumnEvent(
                            testTableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("gender", DataTypes.STRING())))),
                    DataChangeEvent.insertEvent(
                            testTableId,
                            afterDataGenerator.generate(
                                    new Object[] {
                                        3L,
                                        BinaryStringData.fromString("Tom"),
                                        28,
                                        BinaryStringData.fromString("male")
                                    })));

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(arrayOutputStream));
        System.setErr(new PrintStream(arrayErrorStream));
    }

    @AfterEach
    void tearDown() {
        if (System.out != originalSystemOut) {
            System.out.close();
        }
        if (System.err != originalSystemErr) {
            System.err.close();
        }
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }

    static Stream<ValuesDataSink.SinkApi> parameters() {
        return Stream.of(ValuesDataSink.SinkApi.values());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testPrintSinkStdOut(ValuesDataSink.SinkApi sinkType) throws Exception {
        ValuesDataSink valuesDataSink =
                new ValuesDataSink(false, true, sinkType, false, false, true, 100);
        if (sinkType == SINK_V2) {
            Sink<Event> printSink =
                    ((FlinkSinkProvider) valuesDataSink.getEventSinkProvider()).getSink();
            try (SinkWriter<Event> writer = printSink.createWriter(new MockInitContext(1))) {
                MockContext mockContext = new MockContext();
                for (Event event : testData) {
                    writer.write(event, mockContext);
                }
            }
        } else if (sinkType == SINK_FUNCTION) {
            SinkFunction<Event> printSink =
                    ((FlinkSinkFunctionProvider) valuesDataSink.getEventSinkProvider())
                            .getSinkFunction();
            for (Event event : testData) {
                printSink.invoke(event, SinkContextUtil.forTimestamp(0));
            }
        }
        assertThat(arrayOutputStream.toString())
                .isEqualTo(
                        "CreateTableEvent{tableId=testDB.testTable, schema=columns={`id` BIGINT,`name` STRING,`age` INT}, primaryKeys=, options=()}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[1, Alice, 18], op=INSERT, meta=()}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[2, Charlie, 28], op=INSERT, meta=()}\n"
                                + "AddColumnEvent{tableId=testDB.testTable, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=LAST, existedColumnName=null}]}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[3, Tom, 28, male], op=INSERT, meta=()}\n");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void testPrintSinkStdErr(ValuesDataSink.SinkApi sinkType) throws Exception {
        ValuesDataSink valuesDataSink =
                new ValuesDataSink(false, true, sinkType, false, true, true, 100);
        if (sinkType == SINK_V2) {
            Sink<Event> printSink =
                    ((FlinkSinkProvider) valuesDataSink.getEventSinkProvider()).getSink();
            try (SinkWriter<Event> writer = printSink.createWriter(new MockInitContext(1))) {
                MockContext mockContext = new MockContext();
                for (Event event : testData) {
                    writer.write(event, mockContext);
                }
            }
        } else if (sinkType == SINK_FUNCTION) {
            SinkFunction<Event> printSink =
                    ((FlinkSinkFunctionProvider) valuesDataSink.getEventSinkProvider())
                            .getSinkFunction();
            for (Event event : testData) {
                printSink.invoke(event, SinkContextUtil.forTimestamp(0));
            }
        }
        assertThat(arrayErrorStream.toString())
                .isEqualTo(
                        "CreateTableEvent{tableId=testDB.testTable, schema=columns={`id` BIGINT,`name` STRING,`age` INT}, primaryKeys=, options=()}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[1, Alice, 18], op=INSERT, meta=()}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[2, Charlie, 28], op=INSERT, meta=()}\n"
                                + "AddColumnEvent{tableId=testDB.testTable, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=LAST, existedColumnName=null}]}\n"
                                + "DataChangeEvent{tableId=testDB.testTable, before=[], after=[3, Tom, 28, male], op=INSERT, meta=()}\n");
    }

    private static class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return System.currentTimeMillis();
        }
    }

    private static class MockInitContext
            implements Sink.InitContext, SerializationSchema.InitializationContext {

        private final int numSubtasks;

        private MockInitContext(int numSubtasks) {
            this.numSubtasks = numSubtasks;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(ValuesDataSink.class.getClassLoader());
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new DummyMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return new TestProcessingTimeService();
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return numSubtasks;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return InternalSinkWriterMetricGroup.mock(new UnregisteredMetricsGroup());
        }

        @Override
        public MetricGroup getMetricGroup() {
            return metricGroup();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return this;
        }
    }

    private static class DummyMailboxExecutor implements MailboxExecutor {

        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {}

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {}

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }
    }
}
