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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;

/** type of {@link SchemaAwareDeserializationSchema} to deserialize {@link Event} for kafka. */
public enum JsonDeserializationType {

    /** Use {@link DebeziumJsonDeserializationSchema} to deserialize. */
    DEBEZIUM_JSON("debezium-json"),

    /** Use {@link CanalJsonDeserializationSchema} to deserialize. */
    CANAL_JSON("canal-json"),

    /**
     * Use {@link JsonContinuousDeserializationSchema} or {@link JsonStaticDeserializationSchema} to
     * deserialize.
     */
    JSON("json");

    private final String value;

    JsonDeserializationType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
