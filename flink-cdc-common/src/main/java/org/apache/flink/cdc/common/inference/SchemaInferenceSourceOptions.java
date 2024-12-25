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

package org.apache.flink.cdc.common.inference;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.description.Description;
import org.apache.flink.cdc.common.configuration.description.ListElement;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;
import static org.apache.flink.cdc.common.configuration.description.TextElement.text;

/** Options for sources need to inference schema, e.g. Kafka, MongoDB. */
public class SchemaInferenceSourceOptions {

    public static final ConfigOption<SchemaInferenceStrategy> SCHEMA_INFERENCE_STRATEGY =
            key("schema.inference.strategy")
                    .enumType(SchemaInferenceStrategy.class)
                    .defaultValue(SchemaInferenceStrategy.CONTINUOUS)
                    .withDescription(
                            Description.builder()
                                    .text("Strategy for schemaless sources to infer schema.")
                                    .linebreak()
                                    .add(
                                            ListElement.list(
                                                    text(
                                                            "CONTINUOUS: Parse and infer schema for each source records, send schema change events if necessary."),
                                                    text(
                                                            "STATIC: Infer schema once initially, parse source records by initial schemas. Never send schema change events.")))
                                    .build());
}
