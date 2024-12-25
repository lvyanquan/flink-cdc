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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
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

/** Tests for {@link CanalJsonContinuousDeserializationSchema}. */
public class CanalJsonContinuousDeserializationSchemaTest
        extends JsonDeserializationSchemaTestBase {

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.STRING())
                    .physicalColumn("other", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    public void testDeserializeNullRow() throws Exception {
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[0], collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, true, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        List<String> errorJsons =
                Arrays.asList(
                        "",
                        "{}",
                        "[1,2,3]",
                        "{\"data\":[{\"id\":1}],\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":1}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"ERROR\"}");

        for (String json : errorJsons) {
            deserializationSchema.deserialize(json.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(deserializationSchema.getTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testNotIgnoreParseError() throws Exception {
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        String jsonWithoutTableId =
                "{\"data\":[{\"id\":1}],\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}";
        String jsonWithErrorData =
                "{\"data\":[{\"id\":1}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"]}";
        String jsonWithErrorType =
                "{\"data\":[{\"id\":1}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"ERROR\"}";

        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithoutTableId.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .hasMessageContaining("Cannot get database or table name in Canal JSON");
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithErrorData.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        jsonWithErrorType.getBytes(StandardCharsets.UTF_8),
                                        collector))
                .isInstanceOf(IOException.class)
                .rootCause()
                .hasMessageContaining("Unknown \"type\" value \"ERROR\"");
    }

    @Test
    public void testDeserialize() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data.txt");
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        TableId tableId = TableId.tableId("inventory", "products2");
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
                        "CreateTableEvent{tableId=inventory.products2, schema=columns={`id` STRING NOT NULL,`name` STRING,`description` STRING,`weight` STRING,`other` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, val1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[102, car battery, 12V car battery, 8.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[106, hammer, null, 1.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[107, rocks, box of assorted rocks, 5.3, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[106, hammer, null, 1.0, val2], after=[106, hammer, 18oz carpenter hammer, 1.0, val0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[107, rocks, box of assorted rocks, 5.3, null], after=[107, rocks, box of assorted rocks, 5.1, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[110, jacket, water resistent white wind breaker, 0.2, null], after=[110, jacket, new water resistent white wind breaker, 0.5, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.18, null], after=[111, scooter, Big 2-wheel scooter , 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.17, null], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[101, scooter, Small 2-wheel scooter, 3.14, null], after=[101, scooter, Small 2-wheel scooter, 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 8.1, null], after=[102, car battery, 12V car battery, 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 5.17, null], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, null], after=[], op=DELETE, meta=()}");

        assertThat(actualEvents).isEqualTo(expectedEvents);
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, true, true, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        String canalJson =
                "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big2-wheel scooter\",\"weight\":5.17}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"DELETE\"}";
        deserializationSchema.deserialize(canalJson.getBytes(StandardCharsets.UTF_8), collector);

        TableId tableId = TableId.tableId("inventory", "products");
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = collector.getList();
        assertThat(events).hasSize(2);
        assertThat(deserializationSchema.getTableSchema(tableId)).isEqualTo(expectedSchema);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(expectedSchema);

        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.DELETE);
        RecordData data = dataChangeEvent.before();
        assertThat(data.getString(0).toString()).isEqualTo("111");
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Big2-wheel scooter");
        assertThat(data.getString(3).toString()).isEqualTo("5.17");
    }

    @Test
    public void testDeserializeWithSchemaChange() throws Exception {
        String canalJson1 =
                "{\"data\":[{\"id\":\"0\",\"name\":\"scooter\",\"description\":null}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}";
        String canalJson2 =
                "{\"data\":[{\"id\":\"1\",\"name\":\"hammer\",\"description\":\"18oz carpenter hammer\"}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}";

        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        TableId tableId = TableId.tableId("inventory", "products");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(canalJson1.getBytes(StandardCharsets.UTF_8), collector);
        List<Event> events = collector.getList();

        assertThat(events.size()).isEqualTo(2);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(schema);
        assertThat(deserializationSchema.getTableSchema(tableId)).isEqualTo(schema);
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);

        collector.clearList();
        deserializationSchema.deserialize(canalJson2.getBytes(StandardCharsets.UTF_8), collector);
        events = collector.getList();
        assertThat(events.size()).isEqualTo(2);
        assertThat(events.get(0))
                .isEqualTo(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        AddColumnEvent.after(
                                                Column.physicalColumn(
                                                        "description", DataTypes.STRING()),
                                                "name"))));
        assertThat(deserializationSchema.getTableSchema(tableId))
                .isEqualTo(
                        SchemaUtils.applySchemaChangeEvent(
                                schema, (SchemaChangeEvent) events.get(0)));
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data.txt");
        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        "^inventory",
                        "^my.*",
                        false,
                        false,
                        TimestampFormat.SQL,
                        ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(deserializationSchema.getTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();

        deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        "^inventory.*",
                        "^product.*",
                        false,
                        false,
                        TimestampFormat.SQL,
                        ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).hasSize(1);
        assertThat(deserializationSchema.getTableSchemaConverters()).hasSize(1);
        assertThat(collector.getList()).isNotEmpty();
    }

    @Test
    public void testDeserializeDataWithUpdateType() throws Exception {
        String canalJson =
                "{"
                        + "    \"data\": ["
                        + "        {"
                        + "            \"id\": \"0\","
                        + "            \"name\": \"test0\","
                        + "            \"description\": \"desc\","
                        + "            \"weight\": 3.14,"
                        + "            \"other\": \"other0\""
                        + "        },"
                        + "        {"
                        + "            \"id\": \"1\","
                        + "            \"name\": \"test1\","
                        + "            \"description\": null,"
                        + "            \"weight\": null,"
                        + "            \"other\": \"other1\""
                        + "        }"
                        + "    ],"
                        + "    \"old\": ["
                        + "        {"
                        + "            \"name\": null,"
                        + "            \"description\": null,"
                        + "            \"weight\": 3"
                        + "        },"
                        + "        {"
                        + "            \"name\": \"old_test1\","
                        + "            \"description\": null,"
                        + "            \"weight\": 1.0"
                        + "        }"
                        + "    ],"
                        + "    \"database\": \"inventory\","
                        + "    \"table\": \"products\","
                        + "    \"pkNames\": [\"id\"],"
                        + "    \"type\": \"UPDATE\""
                        + "}";

        CanalJsonContinuousDeserializationSchema deserializationSchema =
                new CanalJsonContinuousDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        TableId tableId = TableId.tableId("inventory", "products");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .physicalColumn("other", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(canalJson.getBytes(StandardCharsets.UTF_8), collector);
        List<Event> events = collector.getList();

        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(schema);
        assertThat(((CreateTableEvent) events.get(0)).tableId()).isEqualTo(tableId);

        List<String> actualEvents = new ArrayList<>();
        for (Event event : events) {
            actualEvents.add(TestUtil.convertEventToStr(event, schema));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING NOT NULL,`name` STRING,`description` STRING,`weight` DOUBLE,`other` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[0, null, null, 3.0, other0], after=[0, test0, desc, 3.14, other0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[1, old_test1, null, 1.0, other1], after=[1, test1, null, null, other1], op=UPDATE, meta=()}");
        assertThat(actualEvents).isEqualTo(expectedEvents);
    }
}
