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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import javax.annotation.Nullable;

import java.util.Map;

/** A utility class for pipeline connector options. */
public class OptionUtils {

    /** Utility class can not be instantiated. */
    private OptionUtils() {}

    public static final String VVR_DYNAMIC_START_TIME_MS = "table.dynamic.option.*.*.*.startTimeMs";

    /** See https://aliyuque.antfin.com/ververica/connectors/rge6g0 for more details. */
    public static final ConfigOption<Long> VVR_START_TIME_MS =
            ConfigOptions.key("startTimeMs")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Timestamp in millisecond specifying the time when the connector starts consuming from. "
                                    + "This option is only used for specifying startup timestamp by VVP UI and cannot "
                                    + "be used in CREATE TABLE WITH options.");

    public static @Nullable String getStartTimeMs(ReadableConfig config) {
        Map<String, String> flinkConf;
        if (config instanceof Configuration) {
            flinkConf = ((Configuration) config).toMap();
        } else {
            throw new IllegalStateException(
                    "The config to getStartTimeMs method should return a flink Configuration type.");
        }

        return flinkConf.get(VVR_DYNAMIC_START_TIME_MS);
    }
}
