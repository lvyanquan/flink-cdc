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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CanalJsonDeserializationSchema}. */
public class CanalJsonDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.STRING())
                    .physicalColumn("other", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    public void testDeserializeNullRow() throws Exception {
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[0], collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
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
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
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
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
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
        assertThat(events).hasSize(21);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(ORIGINAL_SCHEMA);

        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent1 = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent1.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent1.after();
        assertThat(data.getString(0).toString()).isEqualTo("101");
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Small 2-wheel scooter");
        assertThat(data.getString(3).toString()).isEqualTo("3.14");
        assertThat(data.getString(4).toString()).isEqualTo("val1");

        assertThat(events.get(10)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent2 = (DataChangeEvent) events.get(10);
        assertThat(dataChangeEvent2.op()).isEqualTo(OperationType.UPDATE);
        RecordData before = dataChangeEvent2.before();
        RecordData after = dataChangeEvent2.after();
        assertThat(before.getString(0).toString()).isEqualTo("106");
        assertThat(before.getString(1).toString()).isEqualTo("hammer");
        assertThat(before.getString(2).toString()).isEmpty();
        assertThat(before.getString(3).toString()).isEqualTo("1.0");
        assertThat(before.getString(4).toString()).isEqualTo("val2");
        assertThat(after.getString(0).toString()).isEqualTo("106");
        assertThat(after.getString(1).toString()).isEqualTo("hammer");
        assertThat(after.getString(2).toString()).isEqualTo("18oz carpenter hammer");
        assertThat(after.getString(3).toString()).isEqualTo("1.0");
        assertThat(after.getString(4).toString()).isEqualTo("val0");
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
                        null, null, true, true, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        String canalJson =
                "{\"data\":[{\"id\":111,\"name\":\"scooter\",\"description\":\"Big2-wheel scooter\",\"weight\":5.17}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"DELETE\"}";
        deserializationSchema.deserialize(canalJson.getBytes(StandardCharsets.UTF_8), collector);

        TableId tableId = TableId.tableId("inventory", "products");
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
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

        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        TableId tableId = TableId.tableId("inventory", "products");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
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
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "description", DataTypes.STRING())))));
        assertThat(deserializationSchema.getTableSchema(tableId))
                .isEqualTo(
                        SchemaUtils.applySchemaChangeEvent(
                                schema, (SchemaChangeEvent) events.get(0)));
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
    }

    @Test
    public void testFilteringTables() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data.txt");
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
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
                new CanalJsonDeserializationSchema(
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

        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        TableId tableId = TableId.tableId("inventory", "products");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .physicalColumn("other", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        List<RecordData.FieldGetter> fieldGetters = TestUtil.getFieldGettersBySchema(schema);

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(canalJson.getBytes(StandardCharsets.UTF_8), collector);
        List<Event> events = collector.getList();

        assertThat(events).hasSize(3);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(schema);
        assertThat(((CreateTableEvent) events.get(0)).tableId()).isEqualTo(tableId);

        assertThat(TestUtil.convertEventToStr(events.get(1), fieldGetters))
                .isEqualTo(
                        "DataChangeEvent{tableId=inventory.products, before=[0, null, null, 3.0, other0], after=[0, test0, desc, 3.14, other0], op=UPDATE, meta=()}");
        assertThat(TestUtil.convertEventToStr(events.get(2), fieldGetters))
                .isEqualTo(
                        "DataChangeEvent{tableId=inventory.products, before=[1, old_test1, null, 1.0, other1], after=[1, test1, null, null, other1], op=UPDATE, meta=()}");
    }
}
