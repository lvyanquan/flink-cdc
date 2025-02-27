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

package org.apache.flink.cdc.connectors.kafka.source.split;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PipelineKafkaPartitionSplitSerializer}. */
public class PipelineKafkaPartitionSplitSerializerTest {

    private final Schema schema1 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .build();

    private final Schema schema2 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("array", DataTypes.ARRAY(DataTypes.DOUBLE()))
                    .metadataColumn("meta", DataTypes.TIMESTAMP())
                    .primaryKey("id", "name")
                    .partitionKey("id")
                    .comment("comment for schema2")
                    .options(
                            new HashMap<String, String>() {
                                {
                                    put("option1", "value1");
                                    put("option2", "value2");
                                }
                            })
                    .build();

    private final Schema keySchema =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("key0", DataTypes.STRING())
                    .physicalColumn("key1", DataTypes.STRING(), "comment", "defaultValue")
                    .build();

    @Test
    public void testSerializer() throws IOException {
        String topic = "topic";
        long offsetZero = 0L;
        long normalOffset = 1L;
        TopicPartition topicPartition = new TopicPartition(topic, 1);
        List<Long> stoppingOffsets =
                Lists.newArrayList(
                        KafkaPartitionSplit.COMMITTED_OFFSET,
                        KafkaPartitionSplit.LATEST_OFFSET,
                        offsetZero,
                        normalOffset);
        PipelineKafkaPartitionSplitSerializer splitSerializer =
                new PipelineKafkaPartitionSplitSerializer();

        Map<TableId, Schema> valueTableSchemas = new HashMap<>();
        valueTableSchemas.put(TableId.tableId("test-db1", "test-table1"), schema1);
        valueTableSchemas.put(TableId.tableId("test-db2", "test-table2"), schema2);

        Map<TableId, Schema> keyTableSchemas = new HashMap<>();
        keyTableSchemas.put(TableId.tableId("test-db1", "test-table1"), keySchema);
        keyTableSchemas.put(TableId.tableId("test-db2", "test-table2"), keySchema);

        for (Long stoppingOffset : stoppingOffsets) {
            PipelineKafkaPartitionSplit kafkaPartitionSplit =
                    new PipelineKafkaPartitionSplit(
                            topicPartition, 0, stoppingOffset, valueTableSchemas, keyTableSchemas);
            byte[] serialize = splitSerializer.serialize(kafkaPartitionSplit);

            KafkaPartitionSplit deserializeSplit =
                    splitSerializer.deserialize(splitSerializer.getVersion(), serialize);
            assertThat(deserializeSplit).isEqualTo(kafkaPartitionSplit);
        }

        // test empty table schema
        PipelineKafkaPartitionSplit kafkaPartitionSplit =
                new PipelineKafkaPartitionSplit(
                        topicPartition,
                        0,
                        offsetZero,
                        Collections.emptyMap(),
                        Collections.emptyMap());
        byte[] serialize = splitSerializer.serialize(kafkaPartitionSplit);
        KafkaPartitionSplit deserializeSplit =
                splitSerializer.deserialize(splitSerializer.getVersion(), serialize);
        assertThat(deserializeSplit).isEqualTo(kafkaPartitionSplit);
    }

    @Test
    public void testBackwardCompatibilityV0() throws IOException {
        PipelineKafkaPartitionSplitSerializer splitSerializer =
                new PipelineKafkaPartitionSplitSerializer();

        String topic = "topic";
        long offsetZero = 0L;
        long normalOffset = 1L;
        TopicPartition topicPartition = new TopicPartition(topic, 1);
        List<Long> stoppingOffsets =
                Lists.newArrayList(
                        KafkaPartitionSplit.COMMITTED_OFFSET,
                        KafkaPartitionSplit.LATEST_OFFSET,
                        offsetZero,
                        normalOffset);

        KafkaPartitionSplitSerializer parentSerializer = new KafkaPartitionSplitSerializer();
        PipelineKafkaPartitionSplitSerializer childSerializer =
                new PipelineKafkaPartitionSplitSerializer();

        Map<TableId, Schema> tableSchemas = new HashMap<>();
        tableSchemas.put(TableId.tableId("test-db1", "test-table1"), schema1);
        tableSchemas.put(TableId.tableId("test-db2", "test-table2"), schema2);

        for (Long stoppingOffset : stoppingOffsets) {
            KafkaPartitionSplit split = new KafkaPartitionSplit(topicPartition, 0, stoppingOffset);
            PipelineKafkaPartitionSplit expectedSplit =
                    new PipelineKafkaPartitionSplit(
                            topicPartition,
                            0,
                            stoppingOffset,
                            tableSchemas,
                            Collections.emptyMap());
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

                baos.write(parentSerializer.serialize(split));
                childSerializer.serializeTableSchemas(tableSchemas, out);
                out.flush();
                byte[] bytesV0 = baos.toByteArray();

                assertThat(childSerializer.deserialize(0, bytesV0)).isEqualTo(expectedSplit);
            }
        }
    }
}
