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

package org.apache.flink.cdc.connectors.sls.source.split;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.StringUtils;

import com.alibaba.ververica.connectors.sls.newsource.model.LogStore;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;
import com.alibaba.ververica.connectors.sls.newsource.utils.SerializationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** The serializer for {@link PipelineSlsShardSplit}. */
public class PipelineSlsShardSplitSerializer implements SimpleVersionedSerializer<SlsShardSplit> {
    private static final int CURRENT_VERSION = 0;

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SlsShardSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            out.writeUTF(split.getLogstore().getProject());
            out.writeUTF(split.getLogstore().getLogstore());
            SerializationUtil.serializeShardToBytes(split.getShard(), out);
            out.writeUTF(split.getStartCursor());
            out.writeUTF(Optional.ofNullable(split.getEndCursor()).orElse(""));

            Map<TableId, Schema> tableSchemas = ((PipelineSlsShardSplit) split).getTableSchemas();
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
    public PipelineSlsShardSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {
            LogStore logStore = new LogStore(in.readUTF(), in.readUTF());
            LogStoreShard shard = SerializationUtil.deserializeShardFromBytes(in);
            String startCursor = in.readUTF();
            String endCursor = whitespaceAsNull(in.readUTF());

            int size = in.readInt();
            Map<TableId, Schema> tableSchemas = new HashMap<>(size);
            while (size > 0) {
                tableSchemas.put(
                        tableIdSerializer.deserialize(in), schemaSerializer.deserialize(in));
                size--;
            }

            return new PipelineSlsShardSplit(logStore, shard, startCursor, endCursor, tableSchemas);
        }
    }

    private static String whitespaceAsNull(String content) {
        return StringUtils.isNullOrWhitespaceOnly(content) ? null : content;
    }
}
