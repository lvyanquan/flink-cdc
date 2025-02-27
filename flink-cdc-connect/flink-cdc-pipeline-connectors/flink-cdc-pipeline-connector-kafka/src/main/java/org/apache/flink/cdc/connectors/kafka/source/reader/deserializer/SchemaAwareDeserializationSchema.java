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

package org.apache.flink.cdc.connectors.kafka.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaAware;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * {@link SchemaAwareDeserializationSchema} is a @{link DeserializationSchema} which maintains table
 * schemas.
 */
public interface SchemaAwareDeserializationSchema<T> extends DeserializationSchema<T>, SchemaAware {

    /**
     * Deserialize messages which do not contain table id information by explicitly passing
     * parameter {@link TableId}.
     */
    default void deserialize(byte[] message, TableId tableId, Collector<T> out) throws IOException {
        deserialize(message, out);
    }
}
