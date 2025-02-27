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

package org.apache.flink.cdc.connectors.sls.source.enumerator;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.alibaba.ververica.connectors.sls.newsource.enumerator.SlsEnumState;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import com.alibaba.ververica.connectors.sls.newsource.utils.SerializationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** The serializer for {@link PipelineSlsSourceEnumState}. */
public class PipelineSlsSourceEnumStateSerializer
        implements SimpleVersionedSerializer<SlsEnumState> {
    private static final int CURRENT_VERSION = 0;
    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SlsEnumState pipelineSlsSourceEnumState) throws IOException {
        PipelineSlsSourceEnumState state = (PipelineSlsSourceEnumState) pipelineSlsSourceEnumState;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {

            out.writeInt(state.assignedShards().size());
            for (LogStoreShard shard : state.assignedShards()) {
                SerializationUtil.serializeShardToBytes(shard, out);
            }
            out.writeInt(state.finishedShards().size());
            for (Map.Entry<LogStoreShard, String> entry : state.finishedShards().entrySet()) {
                SerializationUtil.serializeShardToBytes(entry.getKey(), out);
                out.writeUTF(entry.getValue());
            }
            out.writeInt(state.unassignedInitialShards().size());
            for (LogStoreShard shard : state.unassignedInitialShards()) {
                SerializationUtil.serializeShardToBytes(shard, out);
            }
            out.writeBoolean(state.initialDiscoveryFinished());

            Map<TableId, Schema> initialInferredSchemas = state.getInitialInferredSchemas();
            if (initialInferredSchemas == null || initialInferredSchemas.isEmpty()) {
                out.writeInt(0);
            } else {
                out.writeInt(initialInferredSchemas.size());
                for (Map.Entry<TableId, Schema> entry : initialInferredSchemas.entrySet()) {
                    tableIdSerializer.serialize(entry.getKey(), out);
                    schemaSerializer.serialize(entry.getValue(), out);
                }
            }
            out.writeBoolean(state.isInitialSchemaInferenceFinished());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PipelineSlsSourceEnumState deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            final int numAssignedShards = in.readInt();
            Set<LogStoreShard> assignedShards = new HashSet<>(numAssignedShards);
            for (int i = 0; i < numAssignedShards; i++) {
                assignedShards.add(SerializationUtil.deserializeShardFromBytes(in));
            }

            Map<LogStoreShard, String> finishedShards = new HashMap<>();
            final int numFinishedShards = in.readInt();
            for (int i = 0; i < numFinishedShards; i++) {
                finishedShards.put(SerializationUtil.deserializeShardFromBytes(in), in.readUTF());
            }

            final int numUnassignedInitialShards = in.readInt();
            Set<LogStoreShard> unassignedInitialShards = new HashSet<>(numUnassignedInitialShards);
            for (int i = 0; i < numUnassignedInitialShards; i++) {
                unassignedInitialShards.add(SerializationUtil.deserializeShardFromBytes(in));
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
                throw new IOException("Unexpected trailing bytes in serialized topic Shards");
            }

            return new PipelineSlsSourceEnumState(
                    assignedShards,
                    finishedShards,
                    unassignedInitialShards,
                    initialDiscoveryFinished,
                    initialInferredSchemas,
                    initialSchemaInferenceFinished);
        }
    }
}
