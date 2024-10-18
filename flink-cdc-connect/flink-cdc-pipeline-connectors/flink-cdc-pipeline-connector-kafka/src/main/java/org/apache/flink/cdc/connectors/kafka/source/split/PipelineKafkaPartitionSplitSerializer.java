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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** The serializer for {@link PipelineKafkaPartitionSplit}. */
public class PipelineKafkaPartitionSplitSerializer extends KafkaPartitionSplitSerializer {

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public byte[] serialize(KafkaPartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

            out.writeUTF(split.getTopic());
            out.writeInt(split.getPartition());
            out.writeLong(split.getStartingOffset());
            out.writeLong(split.getStoppingOffset().orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET));

            Map<TableId, Schema> tableSchemas =
                    ((PipelineKafkaPartitionSplit) split).getTableSchemas();
            out.writeInt(tableSchemas.size());
            for (Map.Entry<TableId, Schema> entry : tableSchemas.entrySet()) {
                tableIdSerializer.serialize(entry.getKey(), out);
                schemaSerializer.serialize(entry.getValue(), out);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
            DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

            String topic = in.readUTF();
            int partition = in.readInt();
            long offset = in.readLong();
            long stoppingOffset = in.readLong();

            int size = in.readInt();
            Map<TableId, Schema> tableSchemas = new HashMap<>(size);
            while (size > 0) {
                tableSchemas.put(
                        tableIdSerializer.deserialize(in), schemaSerializer.deserialize(in));
                size--;
            }

            return new PipelineKafkaPartitionSplit(
                    new TopicPartition(topic, partition), offset, stoppingOffset, tableSchemas);
        }
    }
}
