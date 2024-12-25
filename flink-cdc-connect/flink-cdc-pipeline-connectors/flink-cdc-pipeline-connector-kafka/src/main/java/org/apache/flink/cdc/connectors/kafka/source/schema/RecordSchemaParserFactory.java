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

package org.apache.flink.cdc.connectors.kafka.source.schema;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonFormatOptions;
import org.apache.flink.formats.common.TimestampFormat;

import java.time.ZoneId;

/**
 * Factory for providing instances of {@link RecordSchemaParser} to parse schema from kafka record.
 */
public class RecordSchemaParserFactory {

    public static RecordSchemaParser createRecordSchemaParser(
            Configuration formatOptions, JsonSerializationType type, ZoneId zoneId) {
        TimestampFormat timestampFormat;
        boolean primitiveAsString;

        switch (type) {
            case DEBEZIUM_JSON:
                timestampFormat = formatOptions.get(DebeziumJsonFormatOptions.TIMESTAMP_FORMAT);
                primitiveAsString =
                        formatOptions.get(
                                DebeziumJsonFormatOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING);
                boolean schemaInclude = formatOptions.get(DebeziumJsonFormatOptions.SCHEMA_INCLUDE);
                return new DebeziumJsonSchemaParser(
                        schemaInclude, primitiveAsString, timestampFormat, zoneId);
            case CANAL_JSON:
                timestampFormat = formatOptions.get(CanalJsonFormatOptions.TIMESTAMP_FORMAT);
                primitiveAsString =
                        formatOptions.get(CanalJsonFormatOptions.INFER_SCHEMA_PRIMITIVE_AS_STRING);
                String database = formatOptions.get(CanalJsonFormatOptions.DATABASE_INCLUDE);
                String table = formatOptions.get(CanalJsonFormatOptions.TABLE_INCLUDE);
                return new CanalJsonSchemaParser(
                        database, table, primitiveAsString, timestampFormat, zoneId);
            default:
                throw new IllegalArgumentException("UnSupport JsonDeserializationType of " + type);
        }
    }
}
