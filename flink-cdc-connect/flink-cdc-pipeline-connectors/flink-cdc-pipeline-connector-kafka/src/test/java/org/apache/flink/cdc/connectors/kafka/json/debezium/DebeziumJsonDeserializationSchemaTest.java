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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DebeziumJsonDeserializationSchema}. */
public class DebeziumJsonDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .build();

    @Test
    public void testTombstoneMessages() throws Exception {
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
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
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeIncludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-include.txt");
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
                        true, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
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
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
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
                                DebeziumJsonDeserializationSchema.REPLICA_IDENTITY_EXCEPTION,
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
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
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
        DebeziumJsonDeserializationSchema deserializationSchema =
                new DebeziumJsonDeserializationSchema(
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
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "price", DataTypes.BIGINT())))));
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
            List<String> lines, DebeziumJsonDeserializationSchema deserializationSchema)
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
        assertThat(events).hasSize(17);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) events.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(ORIGINAL_SCHEMA);

        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent1 = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent1.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent1.after();
        assertThat(data.getLong(0)).isEqualTo(101L);
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Small 2-wheel scooter");
        assertThat(data.getDouble(3)).isEqualTo(3.140000104904175);

        assertThat(events.get(10)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent2 = (DataChangeEvent) events.get(10);
        assertThat(dataChangeEvent2.op()).isEqualTo(OperationType.UPDATE);
        RecordData before = dataChangeEvent2.before();
        RecordData after = dataChangeEvent2.after();
        assertThat(before.getLong(0)).isEqualTo(106L);
        assertThat(before.getString(1).toString()).isEqualTo("hammer");
        assertThat(before.getString(2).toString()).isEqualTo("16oz carpenter's hammer");
        assertThat(before.getDouble(3)).isEqualTo(1d);
        assertThat(after.getLong(0)).isEqualTo(106L);
        assertThat(after.getString(1).toString()).isEqualTo("hammer");
        assertThat(after.getString(2).toString()).isEqualTo("18oz carpenter hammer");
        assertThat(after.getDouble(3)).isEqualTo(1d);
    }
}
