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
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * PipelineKafkaPartitionSplit}. The serializer version is the combination of parent and child
 * version.
 */
public class PipelineKafkaPartitionSplitSerializer extends KafkaPartitionSplitSerializer {
    private static final int VERSION_BITS = 16;

    /** Split VERSION_0 contains a single table schema map. */
    private static final int VERSION_0 = 0;
    /** Split VERSION_1 contains table schemas for key and value respectively. */
    private static final int VERSION_1 = 1;

    private static final int CURRENT_VERSION = 1;

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public int getVersion() {
        int parentVersion = super.getVersion();
        return (CURRENT_VERSION << VERSION_BITS) + parentVersion;
    }

    @Override
    public byte[] serialize(KafkaPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

            baos.write(super.serialize(split));

            PipelineKafkaPartitionSplit pipelineSplit = ((PipelineKafkaPartitionSplit) split);
            serializeTableSchemas(pipelineSplit.getValueTableSchemas(), out);
            serializeTableSchemas(pipelineSplit.getKeyTableSchemas(), out);

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        int childVersion = version >> VERSION_BITS;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
            DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

            String topic = in.readUTF();
            int partition = in.readInt();
            long offset = in.readLong();
            long stoppingOffset = in.readLong();

            Map<TableId, Schema> valueTableSchemas = deserializeTableSchemas(in);

            Map<TableId, Schema> keyTableSchemas = new HashMap<>();
            switch (childVersion) {
                case VERSION_0:
                    break;
                case VERSION_1:
                    keyTableSchemas = deserializeTableSchemas(in);
                    break;
                default:
                    throw new IOException(
                            String.format(
                                    "The bytes are serialized with version %d, "
                                            + "while this deserializer only supports version up to %d",
                                    childVersion, CURRENT_VERSION));
            }

            return new PipelineKafkaPartitionSplit(
                    new TopicPartition(topic, partition),
                    offset,
                    stoppingOffset,
                    valueTableSchemas,
                    keyTableSchemas);
        }
    }

    protected void serializeTableSchemas(Map<TableId, Schema> tableSchemas, DataOutputView out)
            throws IOException {
        out.writeInt(tableSchemas.size());
        for (Map.Entry<TableId, Schema> entry : tableSchemas.entrySet()) {
            tableIdSerializer.serialize(entry.getKey(), out);
            schemaSerializer.serialize(entry.getValue(), out);
        }
    }

    protected Map<TableId, Schema> deserializeTableSchemas(DataInputView in) throws IOException {
        int size = in.readInt();
        Map<TableId, Schema> tableSchemas = new HashMap<>(size);
        while (size-- > 0) {
            tableSchemas.put(tableIdSerializer.deserialize(in), schemaSerializer.deserialize(in));
        }
        return tableSchemas;
    }
}
