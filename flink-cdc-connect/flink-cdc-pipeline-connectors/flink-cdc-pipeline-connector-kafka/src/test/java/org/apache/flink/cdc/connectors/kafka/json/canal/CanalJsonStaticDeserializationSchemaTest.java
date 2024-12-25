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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationSchemaTestBase;
import org.apache.flink.cdc.connectors.kafka.json.MockInitializationContext;
import org.apache.flink.formats.common.TimestampFormat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema.OP_INSERT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CanalJsonStaticDeserializationSchema}. */
public class CanalJsonStaticDeserializationSchemaTest extends JsonDeserializationSchemaTestBase {

    public static final TableId TABLE_ID = TableId.tableId("inventory", "products2");

    public static final Schema ORIGINAL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("description", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .primaryKey("id")
                    .build();

    public final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void before() {
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    @Test
    public void testDeserializeNullRow() throws Exception {
        CanalJsonStaticDeserializationSchema deserializationSchema =
                new CanalJsonStaticDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        final SimpleCollector collector = new SimpleCollector();

        deserializationSchema.deserialize(null, collector);
        deserializationSchema.deserialize(new byte[0], collector);
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testIgnoreParseError() throws Exception {
        CanalJsonStaticDeserializationSchema deserializationSchema =
                new CanalJsonStaticDeserializationSchema(
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
        assertThat(deserializationSchema.getStaticTableSchemaHelpers()).isEmpty();
        assertThat(collector.getList()).isEmpty();
    }

    @Test
    public void testDeserializeIllegalData() throws Exception {
        CanalJsonStaticDeserializationSchema deserializationSchema =
                new CanalJsonStaticDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        deserializationSchema.setTableSchema(TABLE_ID, ORIGINAL_SCHEMA);

        String type = OP_INSERT;
        List<String> dataList =
                Collections.singletonList(
                        "{\"id\":1,\"name\":\"test\",\"description\":\"desc\",\"weight\":1.0}");
        List<String> oldList = Collections.emptyList();
        List<String> pkNames = Collections.singletonList("id");
        JsonNode oldField = objectMapper.readTree("");

        // unknown table
        TableId otherTableId = TableId.tableId("other", "other");
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        otherTableId, type, dataList, oldList, pkNames, oldField))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        String.format("Unknown initial schema for table <%s>.", otherTableId));

        // change pk
        List<String> pkNamesChanged = Arrays.asList("id", "name");
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        TABLE_ID,
                                        type,
                                        dataList,
                                        oldList,
                                        pkNamesChanged,
                                        oldField))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        String.format(
                                "Primary keys has changed, initial pk is %s, but current pk is %s",
                                ORIGINAL_SCHEMA.primaryKeys(), pkNamesChanged));

        // unknown type
        assertThatThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        TABLE_ID, "UNKNOWN", dataList, oldList, pkNames, oldField))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(String.format("Unknown \"type\" value \"%s\".", "UNKNOWN"));

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();

        // legal data
        assertThatNoException()
                .isThrownBy(
                        () ->
                                deserializationSchema.deserialize(
                                        TABLE_ID, type, dataList, oldList, pkNames, oldField));
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);
    }

    @Test
    public void testDeserialize() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data.txt");

        CanalJsonStaticDeserializationSchema deserializationSchema =
                new CanalJsonStaticDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());
        deserializationSchema.setTableSchema(TABLE_ID, ORIGINAL_SCHEMA);

        assertThat(deserializationSchema.getStaticTableSchemaHelpers()).hasSize(1);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();

        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }
        assertThat(deserializationSchema.getStaticTableSchemaHelpers()).hasSize(1);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);

        List<String> actualEvents = new ArrayList<>();
        for (Event event : collector.getList()) {
            actualEvents.add(TestUtil.convertEventToStr(event, ORIGINAL_SCHEMA));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products2, schema=columns={`id` BIGINT,`name` STRING,`description` STRING,`weight` DOUBLE}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[102, car battery, 12V car battery, 8.1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[106, hammer, null, 1.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[107, rocks, box of assorted rocks, 5.3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[108, jacket, water resistent black wind breaker, 0.1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[109, spare tire, 24 inch spare tire, 22.2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[106, hammer, null, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[107, rocks, box of assorted rocks, 5.3], after=[107, rocks, box of assorted rocks, 5.1], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[110, jacket, water resistent white wind breaker, 0.2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[110, jacket, water resistent white wind breaker, 0.2], after=[110, jacket, new water resistent white wind breaker, 0.5], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.18], after=[111, scooter, Big 2-wheel scooter , 5.17], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.17], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[101, scooter, Small 2-wheel scooter, 3.14], after=[101, scooter, Small 2-wheel scooter, 5.17], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 8.1], after=[102, car battery, 12V car battery, 5.17], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 5.17], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8], after=[], op=DELETE, meta=()}");

        assertThat(actualEvents).isEqualTo(expectedEvents);
    }

    @Test
    public void testDeserializeWithDifferentInitialSchema() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data-schema-change.txt");
        TableId tableId = TableId.tableId("inventory", "products");
        CanalJsonStaticDeserializationSchema deserializationSchema =
                new CanalJsonStaticDeserializationSchema(
                        null, null, false, false, TimestampFormat.SQL, ZoneId.systemDefault());
        deserializationSchema.open(new MockInitializationContext());

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3], after=[3], op=UPDATE, meta=()}");
        runTest(lines, tableId, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT NOT NULL,`name` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3], after=[3, test3], op=UPDATE, meta=()}");
        runTest(lines, tableId, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` INT NOT NULL,`name` STRING,`weight` DOUBLE}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3.0], after=[3, test3, 3.14], op=UPDATE, meta=()}");
        runTest(lines, tableId, schema, deserializationSchema, expected);

        schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .physicalColumn("other", DataTypes.BOOLEAN())
                        .primaryKey("id")
                        .build();
        expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING NOT NULL,`name` STRING,`weight` STRING,`other` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3, null], after=[3, test3, 3.14, null], op=UPDATE, meta=()}");
        runTest(lines, tableId, schema, deserializationSchema, expected);
    }

    public void runTest(
            List<String> lines,
            TableId tableId,
            Schema schema,
            CanalJsonStaticDeserializationSchema deserializationSchema,
            List<String> expected)
            throws Exception {
        // clear status
        deserializationSchema.getStaticTableSchemaHelpers().clear();
        deserializationSchema.getAlreadySendCreateTableTables().clear();
        // set initial table schema
        deserializationSchema.setTableSchema(tableId, schema);

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
}
