/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.sls.source.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.util.Collector;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;

import java.io.IOException;

/**
 * A class that wraps a {@link SlsLogSchemaAwareDeserializationSchema} as the deserializer for a
 * {@link FastLogGroup}.
 *
 * @param <T> the return type of the deserialization.
 */
public class SlsLogOnlySchemaAwareDeserializationSchemaWrapper<T>
        implements SlsLogGroupSchemaAwareDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;

    private final SlsLogSchemaAwareDeserializationSchema<T> deserializationSchema;

    public SlsLogOnlySchemaAwareDeserializationSchemaWrapper(
            SlsLogSchemaAwareDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void deserialize(FastLogGroup fastLogGroup, Collector<T> out) throws IOException {
        for (FastLog log : fastLogGroup.getLogs()) {
            deserializationSchema.deserialize(log, out);
        }
    }

    @Override
    public void deserialize(FastLogGroup fastLogGroup, TableId tableId, Collector<T> out)
            throws IOException {
        for (FastLog log : fastLogGroup.getLogs()) {
            deserializationSchema.deserialize(log, out);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return deserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        deserializationSchema.setTableSchema(tableId, schema);
    }
}
