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
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testEnumStateSerde() throws IOException {
        Map<TableId, Schema> tableSchemas = new HashMap<>();
        tableSchemas.put(TABLE_ID_1, SCHEMA_1);
        tableSchemas.put(TABLE_ID_2, SCHEMA_2);
        final PipelineKafkaSourceEnumState state =
                new PipelineKafkaSourceEnumState(constructPartitions(), true, tableSchemas, true);

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

        assertThat(restoredState.getInitialInferredSchemas())
                .isEqualTo(state.getInitialInferredSchemas());
        assertThat(restoredState.isInitialSchemaInferenceFinished())
                .isEqualTo(state.isInitialSchemaInferenceFinished());
    }

    @Test
    public void testBackwardCompatibility() throws IOException {
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
        assertThat(state.getInitialInferredSchemas()).isEmpty();
        assertThat(state.isInitialSchemaInferenceFinished()).isTrue();
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
}
