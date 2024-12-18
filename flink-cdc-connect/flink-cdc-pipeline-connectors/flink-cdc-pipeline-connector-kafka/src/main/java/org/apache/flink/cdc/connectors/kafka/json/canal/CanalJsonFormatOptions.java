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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.connectors.kafka.json.JsonFormatOptions;
import org.apache.flink.formats.common.TimestampFormat;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for canal-json format. */
public class CanalJsonFormatOptions {

    /** Common options. */
    public static final ConfigOption<TimestampFormat> TIMESTAMP_FORMAT =
            JsonFormatOptions.TIMESTAMP_FORMAT;

    /** Serialization options. */
    public static final ConfigOption<JsonFormatOptions.MapNullKeyMode> MAP_NULL_KEY_MODE =
            JsonFormatOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            JsonFormatOptions.MAP_NULL_KEY_LITERAL;

    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER =
            JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;

    public static final ConfigOption<Boolean> WRITE_NULL_PROPERTIES =
            JsonFormatOptions.WRITE_NULL_PROPERTIES;

    /** Deserialization options. */
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            JsonFormatOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<Boolean> INFER_SCHEMA_PRIMITIVE_AS_STRING =
            JsonFormatOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING;

    public static final ConfigOption<String> DATABASE_INCLUDE =
            key("database.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific databases changelog rows by regular matching the \"database\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    public static final ConfigOption<String> TABLE_INCLUDE =
            key("table.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific tables changelog rows by regular matching the \"table\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    private CanalJsonFormatOptions() {}
}
