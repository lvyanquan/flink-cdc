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

package org.apache.flink.cdc.connectors.kafka.source.schema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.formats.common.TimestampFormat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CanalJsonSchemaParser}. */
public class CanalJsonSchemaParserTest {
    public CanalJsonSchemaParser schemaParser =
            new CanalJsonSchemaParser(
                    null, null, false, TimestampFormat.SQL, ZoneId.systemDefault());

    @BeforeEach
    public void before() throws Exception {
        schemaParser.open();
    }

    @Test
    public void testParseIllegalJson() throws Exception {
        List<String> illegalJsons =
                Arrays.asList(
                        "",
                        "{}",
                        "[1,2,3]",
                        "{\"data\":[{\"id\":1}],\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}",
                        "{\"data\":null,\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}",
                        "{\"data\":[],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}");

        for (String json : illegalJsons) {
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            assertThat(schemaParser.parseRecordValueSchema(record)).isEmpty();
        }
    }

    @Test
    public void testParseNormalJsons() throws Exception {
        List<String> lines = TestUtil.readLines("canal-data.txt");
        TableId tableId = TableId.tableId("inventory", "products2");

        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .physicalColumn("other", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        for (int i = 0; i < lines.size(); i++) {
            String json = lines.get(i);
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                    schemaParser.parseRecordValueSchema(record);
            if (i == 9) {
                // type: CREATE
                assertThat(tableSchemaOp).isEmpty();
                continue;
            }
            assertThat(tableSchemaOp).isPresent();
            assertThat(tableSchemaOp.get().f0).isEqualTo(tableId);
            if (i < 2) {
                assertThat(tableSchemaOp.get().f1).isEqualTo(schema1);
            } else {
                assertThat(tableSchemaOp.get().f1).isEqualTo(schema2);
            }
        }
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        CanalJsonSchemaParser schemaParserPrimitiveAsString =
                new CanalJsonSchemaParser(
                        null, null, true, TimestampFormat.SQL, ZoneId.systemDefault());
        schemaParserPrimitiveAsString.open();

        String json =
                "{"
                        + "    \"data\": ["
                        + "        {"
                        + "            \"id\": 1,"
                        + "            \"weight\": 3.14,"
                        + "            \"date\": \"2024-10-14\","
                        + "            \"description\": \"18oz carpenter hammer\""
                        + "        }"
                        + "    ],"
                        + "    \"database\": \"inventory\","
                        + "    \"table\": \"products\","
                        + "    \"old\": null,"
                        + "    \"pkNames\": ["
                        + "        \"id\""
                        + "    ],"
                        + "    \"type\": \"INSERT\""
                        + "}";
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .physicalColumn("date", DataTypes.DATE())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema schemaAllString =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("weight", DataTypes.STRING())
                        .physicalColumn("date", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        ConsumerRecord<byte[], byte[]> record = buildRecord(json);

        Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                schemaParser.parseRecordValueSchema(record);
        assertThat(tableSchemaOp).isPresent();
        assertThat(tableSchemaOp.get().f1).isEqualTo(schema);

        tableSchemaOp = schemaParserPrimitiveAsString.parseRecordValueSchema(record);
        assertThat(tableSchemaOp).isPresent();
        assertThat(tableSchemaOp.get().f1).isEqualTo(schemaAllString);
    }

    private ConsumerRecord<byte[], byte[]> buildRecord(String json) {
        return new ConsumerRecord<>(
                "test", 0, 0, new byte[0], json.getBytes(StandardCharsets.UTF_8));
    }
}
