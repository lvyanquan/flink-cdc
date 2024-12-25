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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationSchemaTestBase;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.formats.common.TimestampFormat;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonDeserializationSchema.OP_CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DebeziumJsonStaticDeserializationSchemaTest}. */
public class DebeziumJsonStaticDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final TableId TABLE_ID = TableId.tableId("inventory", "products");

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .build();

    @Test
    public void testTombstoneMessages() throws Exception {
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[] {}, collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
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
        assertThat(deserializationSchema.getStaticTableSchemaConverters()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testDeserializeIllegalData() throws Exception {
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
                        false, true, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        deserializationSchema.setTableSchema(TABLE_ID, ORIGINAL_SCHEMA);

        String op = OP_CREATE;
        String before = null;
        String after = "{\"id\":1,\"name\":\"test\",\"description\":\"desc\",\"weight\":1.0}";

        // unknown table
        TableId otherTableId = TableId.tableId("other", "other");
        assertThatThrownBy(() -> deserializationSchema.deserialize(otherTableId, op, before, after))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        String.format("Unknown initial schema for table <%s>.", otherTableId));

        // unknown type
        assertThatThrownBy(
                        () -> deserializationSchema.deserialize(TABLE_ID, "UNKNOWN", before, after))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(String.format("Unknown \"op\" value \"%s\".", "UNKNOWN"));

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();

        // legal data
        assertThatNoException()
                .isThrownBy(() -> deserializationSchema.deserialize(TABLE_ID, op, before, after));
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);
    }

    @Test
    public void testDeserializeExcludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-exclude.txt");
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeIncludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-include.txt");
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
                        true, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        runTest(lines, deserializationSchema);
    }

    @Test
    public void testDeserializeWithDifferentInitialSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-change.txt");
        DebeziumJsonStaticDeserializationSchema deserializationSchema =
                new DebeziumJsonStaticDeserializationSchema(
                        false, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3], after=[3], op=UPDATE, meta=()}");
        runTest(lines, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT,`name` STRING}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3], after=[3, test3], op=UPDATE, meta=()}");
        runTest(lines, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT,`name` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3.0], after=[3, test3, 3.14], op=UPDATE, meta=()}");
        runTest(lines, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .physicalColumn("other", DataTypes.BOOLEAN())
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING,`name` STRING,`weight` STRING,`other` BOOLEAN}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3, null], after=[3, test3, 3.14, null], op=UPDATE, meta=()}");
        runTest(lines, schema, deserializationSchema, expected);
    }

    public void runTest(
            List<String> lines,
            Schema schema,
            DebeziumJsonStaticDeserializationSchema deserializationSchema,
            List<String> expected)
            throws Exception {
        // clear status
        deserializationSchema.getStaticTableSchemaConverters().clear();
        deserializationSchema.getAlreadySendCreateTableTables().clear();
        // set initial table schema
        deserializationSchema.setTableSchema(TABLE_ID, schema);

        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        List<String> actualEvents = new ArrayList<>();
        for (Event event : collector.getList()) {
            actualEvents.add(TestUtil.convertEventToStr(event, schema));
        }
        assertThat(actualEvents).isEqualTo(expected);
    }

    public void runTest(
            List<String> lines, DebeziumJsonStaticDeserializationSchema deserializationSchema)
            throws Exception {
        deserializationSchema.open(new MockInitializationContext());
        deserializationSchema.setTableSchema(TABLE_ID, ORIGINAL_SCHEMA);

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);

        List<String> actualEvents = new ArrayList<>();
        for (Event event : collector.getList()) {
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
