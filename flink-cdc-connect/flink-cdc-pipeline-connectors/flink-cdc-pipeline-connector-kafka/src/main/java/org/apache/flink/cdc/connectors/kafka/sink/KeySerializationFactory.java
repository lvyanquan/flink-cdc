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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.json.JsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.serialization.CsvSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.serialization.JsonSerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;

import java.time.ZoneId;

/**
 * Format factory for providing configured instances of {@link SerializationSchema} to convert
 * {@link Event} to byte.
 */
public class KeySerializationFactory {

    /**
     * Creates a configured instance of {@link SerializationSchema} to convert {@link Event} to
     * byte.
     */
    public static SerializationSchema<Event> createSerializationSchema(
            Configuration formatOptions, KeyFormat keyFormat, ZoneId zoneId) {
        switch (keyFormat) {
            case JSON:
                {
                    TimestampFormat timestampFormat =
                            formatOptions.get(JsonFormatOptions.TIMESTAMP_FORMAT);
                    JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                            formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_MODE);
                    String mapNullKeyLiteral =
                            formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_LITERAL);

                    final boolean encodeDecimalAsPlainNumber =
                            formatOptions.get(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
                    final boolean writeNullProperties =
                            formatOptions.get(JsonFormatOptions.WRITE_NULL_PROPERTIES);
                    final boolean ignoreNullFields =
                            formatOptions.get(JsonFormatOptions.ENCODE_IGNORE_NULL_FIELDS);
                    return new JsonSerializationSchema(
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            zoneId,
                            encodeDecimalAsPlainNumber,
                            ignoreNullFields,
                            writeNullProperties);
                }
            case CSV:
                {
                    return new CsvSerializationSchema(zoneId);
                }
            default:
                {
                    throw new IllegalArgumentException("UnSupport key format of " + keyFormat);
                }
        }
    }
}
