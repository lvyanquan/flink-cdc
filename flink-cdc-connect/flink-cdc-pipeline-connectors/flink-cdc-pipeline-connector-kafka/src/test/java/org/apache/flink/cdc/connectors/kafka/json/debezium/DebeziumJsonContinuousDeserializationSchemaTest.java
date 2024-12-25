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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationSchemaTestBase;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.formats.common.TimestampFormat;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DebeziumJsonContinuousDeserializationSchemaTest}. */
public class DebeziumJsonContinuousDeserializationSchemaTest
        extends JsonDeserializationSchemaTestBase {

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .build();

    @Test
    public void testTombstoneMessages() throws Exception {
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[] {}, collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testDeserializeExcludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-exclude.txt");
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeIncludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-include.txt");
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        true, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, true, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        List<String> errorJsons =
                Arrays.asList(
                        "",
                        "{}",
                        "[1,2,3]",
                        "{\"before\":null,\"after\":{\"id\":101},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":101},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"u\"}",
                        "{\"before\":null,\"after\":{\"id\":101},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"error\"}");

        for (String json : errorJsons) {
            deserializationSchema.deserialize(json.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(deserializationSchema.getTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testNotIgnoreParseError() throws Exception {
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        String jsonWithoutTableId = "{\"before\":null,\"after\":{\"id\":101},\"op\":\"c\"}";
        String jsonWithErrorData =
                "{\"before\":null,\"after\":{\"id\":101},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"u\"}";
        String jsonWithErrorOp =
                "{\"before\":null,\"after\":{\"id\":101},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"error\"}";

        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithoutTableId.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .hasMessageContaining("Cannot get database or table name in Debezium JSON");
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithErrorData.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .hasMessageContaining(
                        String.format(
                                DebeziumJsonContinuousDeserializationSchema
                                        .REPLICA_IDENTITY_EXCEPTION,
                                "UPDATE"));
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithErrorOp.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .hasMessageContaining("Unknown \"op\" value \"error\"");

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(deserializationSchema.getTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-exclude.txt");
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, false, true, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        TableId tableId = TableId.tableId("inventory", "products");
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .build();
        assertThat(deserializationSchema.getTableSchemaConverters()).hasSize(1);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).hasSize(1);
        assertThat(deserializationSchema.getTableSchema(tableId)).isEqualTo(expectedSchema);

        List<Event> events = collector.getList();
        assertThat(events).hasSize(17);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(expectedSchema);

        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent.after();
        assertThat(data.getString(0).toString()).isEqualTo("101");
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Small 2-wheel scooter");
        assertThat(data.getString(3).toString()).isEqualTo("3.140000104904175");
    }

    @Test
    public void testDeserializeWithSchemaChange() throws Exception {
        String debeziumJson1 =
                "{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.140000104904175},\"source\":{\"version\":\"1.1.1.Final\",\"connector\":\"mysql\",\"name\":\"dbserver1\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"inventory\",\"table\":\"products\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1589355606100,\"transaction\":null}";
        String debeziumJson2 =
                "{\"before\":null,\"after\":{\"id\":102,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.140000104904175, \"price\": 100},\"source\":{\"version\":\"1.1.1.Final\",\"connector\":\"mysql\",\"name\":\"dbserver1\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"inventory\",\"table\":\"products\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1589355606100,\"transaction\":null}";
        String debeziumJson3 =
                "{\"before\":null,\"after\":{\"id\":103,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":\"30kg\"},\"source\":{\"version\":\"1.1.1.Final\",\"connector\":\"mysql\",\"name\":\"dbserver1\",\"ts_ms\":0,\"snapshot\":\"true\",\"db\":\"inventory\",\"table\":\"products\",\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":154,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1589355606100,\"transaction\":null}";
        DebeziumJsonContinuousDeserializationSchema deserializationSchema =
                new DebeziumJsonContinuousDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        TableId tableId = TableId.tableId("inventory", "products");

        SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(
                debeziumJson1.getBytes(StandardCharsets.UTF_8), collector);
        List<Event> events = collector.getList();
        assertThat(events.size()).isEqualTo(2);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(ORIGINAL_SCHEMA);
        assertThat(deserializationSchema.getTableSchema(tableId)).isEqualTo(ORIGINAL_SCHEMA);
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);

        collector.clearList();
        deserializationSchema.deserialize(
                debeziumJson2.getBytes(StandardCharsets.UTF_8), collector);
        events = collector.getList();
        assertThat(events.size()).isEqualTo(2);
        assertThat(events.get(0))
                .isEqualTo(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        AddColumnEvent.after(
                                                Column.physicalColumn("price", DataTypes.BIGINT()),
                                                "weight"))));
        assertThat(deserializationSchema.getTableSchema(tableId))
                .isEqualTo(
                        SchemaUtils.applySchemaChangeEvent(
                                ORIGINAL_SCHEMA, (SchemaChangeEvent) events.get(0)));
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);

        collector.clearList();
        deserializationSchema.deserialize(
                debeziumJson3.getBytes(StandardCharsets.UTF_8), collector);
        events = collector.getList();
        assertThat(events.size()).isEqualTo(2);
        assertThat(events.get(0))
                .isEqualTo(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("weight", DataTypes.STRING()),
                                Collections.singletonMap("weight", DataTypes.DOUBLE())));
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    private void runTest(
            List<String> lines, DebeziumJsonContinuousDeserializationSchema deserializationSchema)
            throws Exception {
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        TableId tableId = TableId.tableId("inventory", "products");
        assertThat(deserializationSchema.getTableSchemaConverters()).hasSize(1);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).hasSize(1);
        assertThat(deserializationSchema.getTableSchema(tableId)).isEqualTo(ORIGINAL_SCHEMA);

        List<Event> events = collector.getList();
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(ORIGINAL_SCHEMA);

        List<String> actualEvents = new ArrayList<>();
        for (Event event : events) {
            actualEvents.add(TestUtil.convertEventToStr(event, ORIGINAL_SCHEMA));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` BIGINT,`name` STRING,`description` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.140000104904175], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[102, car battery, 12V car battery, 8.100000381469727], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[107, rocks, box of assorted rocks, 5.300000190734863], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.10000000149011612], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.200000762939453], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[107, rocks, box of assorted rocks, 5.300000190734863], after=[107, rocks, box of assorted rocks, 5.099999904632568], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.20000000298023224], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.179999828338623], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[110, jacket, water resistent white wind breaker, 0.20000000298023224], after=[110, jacket, new water resistent white wind breaker, 0.5], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[111, scooter, Big 2-wheel scooter , 5.179999828338623], after=[111, scooter, Big 2-wheel scooter , 5.170000076293945], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[111, scooter, Big 2-wheel scooter , 5.170000076293945], after=[], op=DELETE, meta=()}");

        assertThat(actualEvents).isEqualTo(expectedEvents);
    }
}
