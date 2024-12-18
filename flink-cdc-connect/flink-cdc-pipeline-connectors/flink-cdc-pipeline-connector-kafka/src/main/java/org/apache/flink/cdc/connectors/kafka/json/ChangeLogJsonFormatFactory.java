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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;

import java.time.ZoneId;

/**
 * Format factory for providing configured instances of {@link SerializationSchema} to convert
 * {@link Event} to json.
 */
@Internal
public class ChangeLogJsonFormatFactory {

    /**
     * Creates a configured instance of {@link SerializationSchema} to convert {@link Event} to
     * json.
     *
     * @param formatOptions The format options.
     * @param type The type of json serialization.
     * @return The configured instance of {@link SerializationSchema}.
     */
    public static SerializationSchema<Event> createSerializationSchema(
            Configuration formatOptions, JsonSerializationType type, ZoneId zoneId) {
        TimestampFormat timestampFormat;
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode;
        String mapNullKeyLiteral;
        boolean encodeDecimalAsPlainNumber;
        boolean writeNullProperties;

        switch (type) {
            case DEBEZIUM_JSON:
                {
                    timestampFormat = formatOptions.get(DebeziumJsonFormatOptions.TIMESTAMP_FORMAT);
                    mapNullKeyMode = formatOptions.get(DebeziumJsonFormatOptions.MAP_NULL_KEY_MODE);
                    mapNullKeyLiteral =
                            formatOptions.get(DebeziumJsonFormatOptions.MAP_NULL_KEY_LITERAL);
                    encodeDecimalAsPlainNumber =
                            formatOptions.get(
                                    DebeziumJsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
                    writeNullProperties =
                            formatOptions.get(DebeziumJsonFormatOptions.WRITE_NULL_PROPERTIES);
                    return new DebeziumJsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            writeNullProperties,
                            zoneId);
                }
            case CANAL_JSON:
                {
                    timestampFormat = formatOptions.get(CanalJsonFormatOptions.TIMESTAMP_FORMAT);
                    mapNullKeyMode = formatOptions.get(CanalJsonFormatOptions.MAP_NULL_KEY_MODE);
                    mapNullKeyLiteral =
                            formatOptions.get(CanalJsonFormatOptions.MAP_NULL_KEY_LITERAL);
                    encodeDecimalAsPlainNumber =
                            formatOptions.get(
                                    CanalJsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
                    writeNullProperties =
                            formatOptions.get(CanalJsonFormatOptions.WRITE_NULL_PROPERTIES);
                    return new CanalJsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            writeNullProperties,
                            zoneId);
                }
            default:
                {
                    throw new IllegalArgumentException(
                            "unSupport JsonSerializationType of " + type);
                }
        }
    }

    /**
     * Creates a configured instance of {@link SchemaAwareDeserializationSchema} to convert json to
     * {@link Event}.
     *
     * @param formatOptions The format options.
     * @param type The type of json serialization.
     * @return The configured instance of {@link SchemaAwareDeserializationSchema}.
     */
    public static SchemaAwareDeserializationSchema<Event> createDeserializationSchema(
            Configuration formatOptions, JsonSerializationType type, ZoneId zoneId) {
        TimestampFormat timestampFormat;
        boolean primitiveAsString;
        boolean ignoreParseErrors;

        switch (type) {
            case DEBEZIUM_JSON:
                timestampFormat = formatOptions.get(DebeziumJsonFormatOptions.TIMESTAMP_FORMAT);
                primitiveAsString =
                        formatOptions.get(
                                DebeziumJsonFormatOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING);
                ignoreParseErrors =
                        formatOptions.get(DebeziumJsonFormatOptions.IGNORE_PARSE_ERRORS);
                boolean schemaInclude = formatOptions.get(DebeziumJsonFormatOptions.SCHEMA_INCLUDE);
                return new DebeziumJsonDeserializationSchema(
                        schemaInclude,
                        ignoreParseErrors,
                        primitiveAsString,
                        timestampFormat,
                        zoneId);
            case CANAL_JSON:
                String database = formatOptions.get(CanalJsonFormatOptions.DATABASE_INCLUDE);
                String table = formatOptions.get(CanalJsonFormatOptions.TABLE_INCLUDE);
                timestampFormat = formatOptions.get(CanalJsonFormatOptions.TIMESTAMP_FORMAT);
                primitiveAsString =
                        formatOptions.get(CanalJsonFormatOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING);
                ignoreParseErrors = formatOptions.get(CanalJsonFormatOptions.IGNORE_PARSE_ERRORS);
                return new CanalJsonDeserializationSchema(
                        database,
                        table,
                        ignoreParseErrors,
                        primitiveAsString,
                        timestampFormat,
                        zoneId);
            default:
                throw new IllegalArgumentException("UnSupport JsonDeserializationType of " + type);
        }
    }
}
