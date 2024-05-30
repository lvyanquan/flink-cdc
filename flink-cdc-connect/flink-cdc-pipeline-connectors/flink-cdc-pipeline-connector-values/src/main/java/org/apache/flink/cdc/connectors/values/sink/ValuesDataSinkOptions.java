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

package org.apache.flink.cdc.connectors.values.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Configurations for {@link ValuesDataSink}. */
public class ValuesDataSinkOptions {

    public static final ConfigOption<Boolean> MATERIALIZED_IN_MEMORY =
            ConfigOptions.key("materialized.in.memory")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True if the DataChangeEvent need to be materialized in memory.");

    public static final ConfigOption<Boolean> PRINT_ENABLED =
            ConfigOptions.key("print.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("True if the Event should be print to console.");

    public static final ConfigOption<ValuesDataSink.SinkApi> SINK_API =
            ConfigOptions.key("sink.api")
                    .enumType(ValuesDataSink.SinkApi.class)
                    .defaultValue(ValuesDataSink.SinkApi.SINK_V2)
                    .withDescription(
                            "The sink api on which the sink is based: SinkFunction or SinkV2.");

    public static final ConfigOption<Boolean> SINK_STANDARD_ERROR =
            ConfigOptions.key("sink.print.standard-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True, if the sink should print to standard error instead of standard out.");

    public static final ConfigOption<Boolean> SINK_LOGGER =
            ConfigOptions.key("sink.print.logger")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("True, if the sink should output through logger.");

    public static final ConfigOption<Long> SINK_LIMIT =
            ConfigOptions.key("sink.print.limit")
                    .longType()
                    .defaultValue(2000L)
                    .withDescription("Limit of logger records to output, default 2000");
}
