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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.formats.common.TimestampFormat;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonStaticDeserializationSchemaTest}. */
public class JsonStaticDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final TableId TABLE_ID = TableId.tableId("test_topic");

    @Test
    public void testTombstoneMessages() throws Exception {
        JsonStaticDeserializationSchema deserializationSchema =
                new JsonStaticDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(null, TABLE_ID, collector);
        deserializationSchema.deserialize(new byte[] {}, TABLE_ID, collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        JsonStaticDeserializationSchema deserializationSchema =
                new JsonStaticDeserializationSchema(
                        false, true, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        deserializationSchema.setTableSchema(TABLE_ID, Schema.newBuilder().build());
        SimpleCollector collector = new SimpleCollector();

        List<String> errorJsons =
                Arrays.asList("", "null", "123.456", "[1,2,3]", "{\"field\"}", "{a:b}");
        for (String json : errorJsons) {
            deserializationSchema.deserialize(
                    json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);
        }

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testDeserialize() throws Exception {
        List<String> jsons =
                Arrays.asList(
                        "{\"id\":1, \"name\":\"test1\"}",
                        "{\"id\":2, \"name\":\"test2\", \"age\": 10, \"phone\": 123}",
                        "{\"id\":3, \"name\":\"test3\", \"age\": 20, \"phone\": \"01-23\"}",
                        "{\"id\":4, \"age\": 30, \"name\": \"test4\"}");

        JsonStaticDeserializationSchema deserializationSchema =
                new JsonStaticDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=test_topic, schema=columns={`id` INT}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[4], op=INSERT, meta=()}");
        runTest(jsons, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=test_topic, schema=columns={`id` INT,`name` STRING}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[2, test2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[3, test3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[4, test4], op=INSERT, meta=()}");
        runTest(jsons, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .physicalColumn("phone", DataTypes.STRING())
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=test_topic, schema=columns={`id` BIGINT,`name` STRING,`age` INT,`phone` STRING}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[2, test2, 10, 123], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[3, test3, 20, 01-23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_topic, before=[], after=[4, test4, 30, null], op=INSERT, meta=()}");
        runTest(jsons, schema, deserializationSchema, expected);
    }

    public void runTest(
            List<String> jsons,
            Schema schema,
            JsonStaticDeserializationSchema deserializationSchema,
            List<String> expected)
            throws Exception {
        deserializationSchema.getStaticTableSchemaConverters().clear();
        deserializationSchema.getAlreadySendCreateTableTables().clear();
        deserializationSchema.setTableSchema(TABLE_ID, schema);

        SimpleCollector collector = new SimpleCollector();
        for (String json : jsons) {
            deserializationSchema.deserialize(
                    json.getBytes(StandardCharsets.UTF_8), TABLE_ID, collector);
        }
        List<String> actualEvents = new ArrayList<>();
        for (Event event : collector.getList()) {
            assertThat(event).isInstanceOfAny(CreateTableEvent.class, DataChangeEvent.class);
            actualEvents.add(TestUtil.convertEventToStr(event, schema));
        }
        assertThat(actualEvents).isEqualTo(expected);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);
    }
}
