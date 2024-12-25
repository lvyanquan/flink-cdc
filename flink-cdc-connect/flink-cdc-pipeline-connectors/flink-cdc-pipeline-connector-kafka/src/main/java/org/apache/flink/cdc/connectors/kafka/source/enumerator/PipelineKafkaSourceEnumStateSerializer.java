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
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.connector.kafka.source.enumerator.AssignmentStatus;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of pipeline Kafka source. The {@link PipelineKafkaSourceEnumState} contains parent state
 * and child state. The version in serializer contains parent version and child version.
 */
public class PipelineKafkaSourceEnumStateSerializer extends KafkaSourceEnumStateSerializer {
    private static final int VERSION_BITS = 16;

    /** State of VERSION_0 is same as KafkaSourceEnumState without additional data. */
    private static final int VERSION_0 = 0;
    /** State of VERSION_1 contains initialInferredSchemas and initialSchemaInferenceFinished. */
    private static final int VERSION_1 = 1;

    private static final int CURRENT_VERSION = 1;

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public int getVersion() {
        // version is combined with parent version (low 16 bits) and child version (high 16 bits).
        int parentVersion = super.getVersion();
        return (CURRENT_VERSION << VERSION_BITS) + parentVersion;
    }

    @Override
    public byte[] serialize(KafkaSourceEnumState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            // serialize parent state
            baos.write(super.serialize(enumState));

            // serialize child state
            PipelineKafkaSourceEnumState pipelineEnumState =
                    (PipelineKafkaSourceEnumState) enumState;
            Map<TableId, Schema> initialInferredSchemas =
                    pipelineEnumState.getInitialInferredSchemas();
            if (initialInferredSchemas == null || initialInferredSchemas.isEmpty()) {
                out.writeInt(0);
            } else {
                out.writeInt(initialInferredSchemas.size());
                for (Map.Entry<TableId, Schema> entry : initialInferredSchemas.entrySet()) {
                    tableIdSerializer.serialize(entry.getKey(), out);
                    schemaSerializer.serialize(entry.getValue(), out);
                }
            }
            out.writeBoolean(pipelineEnumState.isInitialSchemaInferenceFinished());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // parse parent version (low 16 bits) and child version (high 16 bits)
        int childVersion = version >>> VERSION_BITS;
        int parentVersion = ((1 << VERSION_BITS) - 1) & version;

        switch (childVersion) {
            case VERSION_0:
                KafkaSourceEnumState state = super.deserialize(parentVersion, serialized);
                return new PipelineKafkaSourceEnumState(
                        state.partitions(),
                        state.initialDiscoveryFinished(),
                        new HashMap<>(),
                        true);
            case VERSION_1:
                // TODO deserialize logic should be updated after parent version being changed
                return deserializeTopicPartitionAndAssignmentStatusAndInitialInferredSchemas(
                        serialized);
            default:
                throw new IOException(
                        String.format(
                                "The bytes are serialized with version %d, "
                                        + "while this deserializer only supports version up to %d",
                                childVersion, CURRENT_VERSION));
        }
    }

    private PipelineKafkaSourceEnumState
            deserializeTopicPartitionAndAssignmentStatusAndInitialInferredSchemas(byte[] serialized)
                    throws IOException {

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            final int numPartitions = in.readInt();
            Set<TopicPartitionAndAssignmentStatus> partitions = new HashSet<>(numPartitions);

            for (int i = 0; i < numPartitions; i++) {
                final String topic = in.readUTF();
                final int partition = in.readInt();
                final int statusCode = in.readInt();
                partitions.add(
                        new TopicPartitionAndAssignmentStatus(
                                new TopicPartition(topic, partition),
                                AssignmentStatus.ofStatusCode(statusCode)));
            }
            final boolean initialDiscoveryFinished = in.readBoolean();

            final int numInitialInferredSchemas = in.readInt();
            Map<TableId, Schema> initialInferredSchemas = new HashMap<>(numInitialInferredSchemas);
            for (int i = 0; i < numInitialInferredSchemas; i++) {
                initialInferredSchemas.put(
                        tableIdSerializer.deserialize(in), schemaSerializer.deserialize(in));
            }
            final boolean initialSchemaInferenceFinished = in.readBoolean();

            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes.");
            }

            return new PipelineKafkaSourceEnumState(
                    partitions,
                    initialDiscoveryFinished,
                    initialInferredSchemas,
                    initialSchemaInferenceFinished);
        }
    }
}
