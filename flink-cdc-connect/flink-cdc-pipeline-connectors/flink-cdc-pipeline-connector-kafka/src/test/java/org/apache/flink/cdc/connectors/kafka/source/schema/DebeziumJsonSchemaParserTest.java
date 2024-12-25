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

/** Tests for {@link DebeziumJsonSchemaParser}. */
public class DebeziumJsonSchemaParserTest {

    public DebeziumJsonSchemaParser schemaParserIncludeSchema =
            new DebeziumJsonSchemaParser(true, false, TimestampFormat.SQL, ZoneId.systemDefault());

    public DebeziumJsonSchemaParser schemaParserExcludeSchema =
            new DebeziumJsonSchemaParser(false, false, TimestampFormat.SQL, ZoneId.systemDefault());

    @BeforeEach
    public void before() throws Exception {
        schemaParserIncludeSchema.open();
        schemaParserExcludeSchema.open();
    }

    @Test
    public void testParseIllegalJson() throws Exception {
        List<String> illegalJsons =
                Arrays.asList(
                        "",
                        "{}",
                        "[1,2,3]",
                        "{\"before\":null,\"after\":{\"id\":101},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":101},\"source\":{\"db\":\"inventory\"},\"op\":\"u\"}",
                        "{\"before\":null,\"after\":null,\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"c\"}");

        for (String json : illegalJsons) {
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            assertThat(schemaParserIncludeSchema.parseRecordSchema(record)).isEmpty();
            assertThat(schemaParserExcludeSchema.parseRecordSchema(record)).isEmpty();
        }
    }

    @Test
    public void testParseExcludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-exclude.txt");
        TableId tableId = TableId.tableId("inventory", "products");

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();

        for (String json : lines) {
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                    schemaParserExcludeSchema.parseRecordSchema(record);
            assertThat(tableSchemaOp).isPresent();
            assertThat(tableSchemaOp.get().f0).isEqualTo(tableId);
            assertThat(tableSchemaOp.get().f1).isEqualTo(schema);
        }
    }

    @Test
    public void testParseIncludeSchema() throws Exception {
        List<String> lines = TestUtil.readLines("debezium-data-schema-include.txt");
        TableId tableId = TableId.tableId("inventory", "products");

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();

        for (String json : lines) {
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                    schemaParserIncludeSchema.parseRecordSchema(record);
            assertThat(tableSchemaOp).isPresent();
            assertThat(tableSchemaOp.get().f0).isEqualTo(tableId);
            assertThat(tableSchemaOp.get().f1).isEqualTo(schema);
        }
    }

    @Test
    public void testPrimitiveAsString() throws Exception {
        DebeziumJsonSchemaParser schemaParser =
                new DebeziumJsonSchemaParser(
                        true, true, TimestampFormat.SQL, ZoneId.systemDefault());
        schemaParser.open();

        List<String> lines = TestUtil.readLines("debezium-data-schema-include.txt");
        TableId tableId = TableId.tableId("inventory", "products");

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .build();

        for (String json : lines) {
            ConsumerRecord<byte[], byte[]> record = buildRecord(json);
            Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                    schemaParser.parseRecordSchema(record);
            assertThat(tableSchemaOp).isPresent();
            assertThat(tableSchemaOp.get().f0).isEqualTo(tableId);
            assertThat(tableSchemaOp.get().f1).isEqualTo(schema);
        }
    }

    private ConsumerRecord<byte[], byte[]> buildRecord(String json) {
        return new ConsumerRecord<>(
                "test", 0, 0, new byte[0], json.getBytes(StandardCharsets.UTF_8));
    }
}
