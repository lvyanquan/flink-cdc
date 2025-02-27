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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.formats.common.TimestampFormat;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonContinuousDeserializationSchemaTest}. */
public class JsonContinuousDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final TableId TABLE_ID = TableId.tableId("test_topic");

    @Test
    public void testTombstoneMessages() throws Exception {
        JsonContinuousDeserializationSchema deserializationSchema =
                new JsonContinuousDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(null, TABLE_ID, collector);
        deserializationSchema.deserialize(new byte[] {}, TABLE_ID, collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        JsonContinuousDeserializationSchema deserializationSchema =
                new JsonContinuousDeserializationSchema(
                        false, true, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        List<String> errorJsons =
                Arrays.asList("", "null", "123.456", "[1,2,3]", "{\"field\"}", "{a:b}");
        for (String json : errorJsons) {
            deserializationSchema.deserialize(
                    json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);
        }

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(deserializationSchema.getTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        String json =
                "{"
                        + "\"f0\": null, "
                        + "\"f1\": 10, "
                        + "\"f2\": 3.14, "
                        + "\"f3\": [1,2,3], "
                        + "\"f4\": \"flink\", "
                        + "\"f5\": {\"k0\": 1, \"k1\": \"value\"} "
                        + "}";
        JsonContinuousDeserializationSchema deserializationSchema =
                new JsonContinuousDeserializationSchema(
                        false, false, true, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(
                json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f2", DataTypes.STRING())
                        .physicalColumn("f3", DataTypes.STRING())
                        .physicalColumn("f4", DataTypes.STRING())
                        .physicalColumn("f5", DataTypes.STRING())
                        .build();
        assertThat(deserializationSchema.getTableSchemaConverters()).hasSize(1);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).hasSize(1);
        assertThat(deserializationSchema.getTableSchema(TABLE_ID)).isEqualTo(expectedSchema);

        List<Event> events = collector.getList();
        assertThat(events).hasSize(2);

        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(expectedSchema);
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent.after();
        assertThat(data.getString(0).toString()).isEqualTo("10");
        assertThat(data.getString(1).toString()).isEqualTo("3.14");
        assertThat(data.getString(2).toString()).isEqualTo("[1,2,3]");
        assertThat(data.getString(3).toString()).isEqualTo("flink");
        assertThat(data.getString(4).toString()).isEqualTo("{\"k0\":1,\"k1\":\"value\"}");
    }

    @Test
    public void testFlattenNestedColumn() throws Exception {
        String json =
                "{"
                        + "\"f0\": null, "
                        + "\"f1\": 3.14, "
                        + "\"f2\": {\"key\": \"value\"}, "
                        + "\"f3\": {\"k1\": 10, \"k2\": {\"arr\": [1,2,3]}} "
                        + "}";
        JsonContinuousDeserializationSchema deserializationSchema =
                new JsonContinuousDeserializationSchema(
                        true, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(
                json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);

        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("f1", DataTypes.DOUBLE())
                        .physicalColumn("f2.key", DataTypes.STRING())
                        .physicalColumn("f3.k1", DataTypes.BIGINT())
                        .physicalColumn("f3.k2.arr", DataTypes.STRING())
                        .build();

        List<Event> events = collector.getList();
        assertThat(events).hasSize(2);
        assertThat(events.get(0)).isInstanceOf(CreateTableEvent.class);
        assertThat(((CreateTableEvent) events.get(0)).getSchema()).isEqualTo(expectedSchema);
        assertThat(events.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent = (DataChangeEvent) events.get(1);
        assertThat(dataChangeEvent.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent.after();
        assertThat(data.getDouble(0)).isEqualTo(3.14);
        assertThat(data.getString(1).toString()).isEqualTo("value");
        assertThat(data.getLong(2)).isEqualTo(10);
        assertThat(data.getString(3).toString()).isEqualTo("[1,2,3]");
    }

    @Test
    public void testDeserializeWithSchemaChange() throws Exception {
        List<String> jsons =
                Arrays.asList(
                        "{\"id\":1, \"name\":\"test1\"}",
                        "{\"id\":2, \"name\":\"test2\", \"age\": 10, \"phone\": 123}",
                        "{\"id\":3, \"name\":\"test3\", \"age\": 20, \"phone\": \"01-23\"}",
                        "{\"id\":4, \"age\": 30, \"name\": \"test4\"}");

        JsonContinuousDeserializationSchema deserializationSchema =
                new JsonContinuousDeserializationSchema(
                        true, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        SimpleCollector collector = new SimpleCollector();

        for (String json : jsons) {
            deserializationSchema.deserialize(
                    json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);
        }
        List<Event> actual = collector.getList();
        Schema schema = null;

        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                schema = SchemaUtils.applySchemaChangeEvent(schema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, schema));
        }
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=test_topic, schema=columns={`id` BIGINT,`name` STRING}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=test_topic, addedColumns=[ColumnWithPosition{column=`age` BIGINT, position=AFTER, existedColumnName=name}, ColumnWithPosition{column=`phone` BIGINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[2, test2, 10, 123], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=test_topic, typeMapping={phone=STRING}, oldTypeMapping={phone=BIGINT}}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[3, test3, 20, 01-23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[4, test4, 30, null], op=INSERT, meta=()}");
        assertThat(actualReadableEvents).isEqualTo(expected);
    }
}
