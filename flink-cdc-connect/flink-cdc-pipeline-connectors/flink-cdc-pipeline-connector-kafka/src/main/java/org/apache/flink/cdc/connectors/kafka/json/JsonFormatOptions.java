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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.formats.common.TimestampFormat;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for json format. */
public class JsonFormatOptions {

    public static final ConfigOption<TimestampFormat> TIMESTAMP_FORMAT =
            key("timestamp-format.standard")
                    .enumType(TimestampFormat.class)
                    .defaultValue(TimestampFormat.SQL)
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static final ConfigOption<MapNullKeyMode> MAP_NULL_KEY_MODE =
            key("map-null-key.mode")
                    .enumType(MapNullKeyMode.class)
                    .defaultValue(MapNullKeyMode.FAIL)
                    .withDescription(
                            "Optional flag to control the handling mode when serializing null key for map data, FAIL by default."
                                    + " Option DROP will drop null key entries for map data."
                                    + " Option LITERAL will use 'map-null-key.literal' as key literal.");

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            key("map-null-key.literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");

    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER =
            key("encode.decimal-as-plain-number")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to encode all decimals as plain numbers instead of possible scientific notations, false by default.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if parse error, false by default.");

    public static final ConfigOption<Boolean> INFER_SCHEMA_FLATTEN_NECOLUMNS_ENABLE =
            key("infer-schema.flatten-nested-columns.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to recursively flatten the JSON OBJECT field into columns in the top level schema. "
                                    + "For a better understanding, the name of the flattened column will be composed of the path to get the column. "
                                    + "For example, the field `col` in the json {\"nested\": {\"col\": true}} is `nested.col` in the flattened schema. "
                                    + "If the flattened field names get conflicts, currently the JsonParser will throw the exception to stop the execution. "
                                    + "Please use the option 'json.ignore-parse-errors' to ignore the parse errors.");

    public static final ConfigOption<Boolean> INFER_SCHEMA_PRIMITIVE_AS_STRING =
            key("infer-schema.primitive-as-string")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional flag to infer primitive types as string type.");

    public static final ConfigOption<Boolean> WRITE_NULL_PROPERTIES =
            key("write-null-properties")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to write null properties when serializing");

    public static final ConfigOption<Boolean> ENCODE_IGNORE_NULL_FIELDS =
            key("encode.ignore-null-fields")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to ignore null fields when encoding, false by default.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Handling mode for map data with null key. */
    public enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL
    }

    private JsonFormatOptions() {}
}
