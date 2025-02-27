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

package org.apache.flink.cdc.connectors.kafka.source.enumerator;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PipelineKafkaSourceEnumStateSerializer}. */
public class PipelineKafkaSourceEnumStateSerializerTest {

    private static final TableId TABLE_ID_1 = TableId.tableId("test-topic");

    private static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final TableId TABLE_ID_2 = TableId.tableId("inventory", "products");
    private static final Schema SCHEMA_2 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .build();

    private static final Schema SCHEMA_KEY =
            Schema.newBuilder()
                    .physicalColumn("key1", DataTypes.BIGINT())
                    .physicalColumn("key2", DataTypes.STRING())
                    .build();

    @Test
    public void testEnumStateSerde() throws IOException {
        Map<TableId, Schema> valueTableSchemas = new HashMap<>();
        valueTableSchemas.put(TABLE_ID_1, SCHEMA_1);
        valueTableSchemas.put(TABLE_ID_2, SCHEMA_2);
        Map<TableId, Schema> keyTableSchemas = new HashMap<>();
        keyTableSchemas.put(TABLE_ID_1, SCHEMA_KEY);
        keyTableSchemas.put(TABLE_ID_2, SCHEMA_KEY);

        final PipelineKafkaSourceEnumState state =
                new PipelineKafkaSourceEnumState(
                        constructPartitions(), true, keyTableSchemas, valueTableSchemas, true);

        final PipelineKafkaSourceEnumStateSerializer serializer =
                new PipelineKafkaSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);
        final PipelineKafkaSourceEnumState restoredState =
                (PipelineKafkaSourceEnumState)
                        serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restoredState.assignedPartitions()).isEqualTo(state.assignedPartitions());
        assertThat(restoredState.unassignedInitialPartitions())
                .isEqualTo(state.unassignedInitialPartitions());
        assertThat(restoredState.initialDiscoveryFinished()).isTrue();

        assertThat(restoredState.getInitialInferredValueSchemas())
                .isEqualTo(state.getInitialInferredValueSchemas());
        assertThat(restoredState.getInitialInferredKeySchemas())
                .isEqualTo(state.getInitialInferredKeySchemas());
        assertThat(restoredState.isInitialSchemaInferenceFinished())
                .isEqualTo(state.isInitialSchemaInferenceFinished());
    }

    @Test
    public void testBackwardCompatibilityV0() throws IOException {
        KafkaSourceEnumState parentState = new KafkaSourceEnumState(constructPartitions(), true);
        KafkaSourceEnumStateSerializer parentSerializer = new KafkaSourceEnumStateSerializer();

        final byte[] parentStateBytes = parentSerializer.serialize(parentState);

        PipelineKafkaSourceEnumStateSerializer serializer =
                new PipelineKafkaSourceEnumStateSerializer();
        PipelineKafkaSourceEnumState state =
                (PipelineKafkaSourceEnumState)
                        serializer.deserialize(parentSerializer.getVersion(), parentStateBytes);

        assertThat(state.assignedPartitions()).isEqualTo(parentState.assignedPartitions());
        assertThat(state.initialDiscoveryFinished())
                .isEqualTo(parentState.initialDiscoveryFinished());
        assertThat(state.getInitialInferredKeySchemas()).isEmpty();
        assertThat(state.getInitialInferredValueSchemas()).isEmpty();
        assertThat(state.isInitialSchemaInferenceFinished()).isTrue();
    }

    @Test
    public void testBackwardCompatibilityV1() throws IOException {
        Map<TableId, Schema> initialInferredValueSchemas = new HashMap<>();
        initialInferredValueSchemas.put(TABLE_ID_1, SCHEMA_1);
        initialInferredValueSchemas.put(TABLE_ID_2, SCHEMA_2);
        PipelineKafkaSourceEnumState stateV1 =
                new PipelineKafkaSourceEnumState(
                        constructPartitions(),
                        true,
                        new HashMap<>(),
                        initialInferredValueSchemas,
                        true);
        PipelineKafkaSourceEnumStateSerializerV1 serializerV1 =
                new PipelineKafkaSourceEnumStateSerializerV1();
        byte[] bytes = serializerV1.serialize(stateV1);

        PipelineKafkaSourceEnumStateSerializer serializer =
                new PipelineKafkaSourceEnumStateSerializer();
        PipelineKafkaSourceEnumState state =
                (PipelineKafkaSourceEnumState)
                        serializer.deserialize(serializerV1.getVersion(), bytes);

        assertThat(state.assignedPartitions()).isEqualTo(stateV1.assignedPartitions());
        assertThat(state.initialDiscoveryFinished()).isEqualTo(stateV1.initialDiscoveryFinished());
        assertThat(state.getInitialInferredKeySchemas()).isEmpty();
        assertThat(state.getInitialInferredValueSchemas())
                .isEqualTo(stateV1.getInitialInferredValueSchemas());
        assertThat(state.isInitialSchemaInferenceFinished())
                .isEqualTo(stateV1.isInitialSchemaInferenceFinished());
    }

    private Set<TopicPartitionAndAssignmentStatus> constructPartitions() {
        Set<TopicPartitionAndAssignmentStatus> partitions = new HashSet<>();
        partitions.add(
                new TopicPartitionAndAssignmentStatus(
                        new TopicPartition("topic-0", 0), AssignmentStatus.ASSIGNED));
        partitions.add(
                new TopicPartitionAndAssignmentStatus(
                        new TopicPartition("topic-1", 1), AssignmentStatus.UNASSIGNED_INITIAL));
        return partitions;
    }

    private static class PipelineKafkaSourceEnumStateSerializerV1
            extends KafkaSourceEnumStateSerializer {
        private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
        private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

        @Override
        public int getVersion() {
            return (1 << 16) + super.getVersion();
        }

        @Override
        public byte[] serialize(KafkaSourceEnumState enumState) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
                baos.write(super.serialize(enumState));

                PipelineKafkaSourceEnumState pipelineEnumState =
                        (PipelineKafkaSourceEnumState) enumState;
                // only serialize value schemas
                serializeSchemas(pipelineEnumState.getInitialInferredValueSchemas(), out);
                out.writeBoolean(pipelineEnumState.isInitialSchemaInferenceFinished());

                out.flush();
                return baos.toByteArray();
            }
        }

        private void serializeSchemas(
                Map<TableId, Schema> tableSchemas, DataOutputViewStreamWrapper out)
                throws IOException {
            if (tableSchemas == null) {
                out.writeInt(0);
            } else {
                out.writeInt(tableSchemas.size());
                for (Map.Entry<TableId, Schema> entry : tableSchemas.entrySet()) {
                    tableIdSerializer.serialize(entry.getKey(), out);
                    schemaSerializer.serialize(entry.getValue(), out);
                }
            }
        }
    }
}
