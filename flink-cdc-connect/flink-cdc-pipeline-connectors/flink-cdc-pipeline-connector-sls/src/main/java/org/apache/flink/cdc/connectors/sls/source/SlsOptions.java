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

package org.apache.flink.cdc.connectors.sls.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;

import com.alibaba.ververica.connectors.sls.source.StartupMode;

import java.time.Duration;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for {@link SlsDataSource}. */
public class SlsOptions {

    public static final ConfigOption<String> ENDPOINT =
            key("endpoint".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS endpoint address.");

    public static final ConfigOption<String> ACCESS_ID =
            key("accessId".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS access key ID.");

    public static final ConfigOption<String> ACCESS_KEY =
            key("accessKey".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS access key secret.");

    public static final ConfigOption<String> PROJECT =
            key("project".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS project name.");

    public static final ConfigOption<String> LOGSTORE =
            key("logStore".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS logStore name or metricStore name.");

    public static final ConfigOption<Integer> SCAN_MAX_PRE_FETCH_LOG_GROUPS =
            key("maxPreFetchLogGroups".toLowerCase())
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The max number of log groups to be parsed for SLS logstore in order to get the initial table schema.");

    public static final ConfigOption<Long> SHARD_DISCOVERY_INTERVAL_MS =
            key("shardDiscoveryIntervalMs".toLowerCase())
                    .longType()
                    .defaultValue(Duration.ofMinutes(1).toMillis())
                    .withDescription(
                            "The interval in milliseconds for the SLS source to discover "
                                    + "the shard state change. A non-positive value disables the shard discovery.");

    public static final ConfigOption<StartupMode> STARTUP_MODE =
            key("startupMode".toLowerCase())
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.TIMESTAMP)
                    .withDescription("SLS source startup mode.");

    public static final ConfigOption<String> START_TIME =
            key("startTime".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Date-time string with ISO-8601 format specifying the time when the connector starts consuming from.");

    public static final ConfigOption<String> STOP_TIME =
            key("stopTime".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Date-time string with ISO-8601 format specifying the time when the connector starts consuming to.");

    public static final ConfigOption<String> CONSUMER_GROUP =
            key("consumerGroup".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS consumer group to save cursors for shards.");

    public static final ConfigOption<Integer> BATCH_GET_SIZE =
            key("bathGetSize".toLowerCase())
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "SLS source batch get size. The value should be smaller than 1000.");

    public static final ConfigOption<Integer> MAX_RETRIES =
            key("maxRetries".toLowerCase())
                    .intType()
                    .defaultValue(3)
                    .withDescription("Max retry times when failed to read data from SLS.");

    public static final ConfigOption<Boolean> EXIT_AFTER_FINISH =
            key("exitAfterFinish".toLowerCase())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Exit flink job after consuming finished or not");

    public static final ConfigOption<String> QUERY =
            key("query".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SLS query using SPL language to filter data before consuming from connector.");

    public static final ConfigOption<String> COMPRESS_TYPE =
            key("compressType".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS compress type.");

    public static final ConfigOption<String> TIME_ZONE =
            key("timeZone".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Time zone for start time/stop time.");

    public static final ConfigOption<String> REGION_ID =
            key("regionId".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS region ID.");

    public static final ConfigOption<String> SIGN_VERSION =
            key("signVersion".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SLS sign version.");

    public static final ConfigOption<Integer> SHARD_MOD_DIVISOR =
            key("shardModDivisor".toLowerCase())
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "SLS shard mod divisor. Using it with shard mod remainder to read part of shards for a logstore.");

    public static final ConfigOption<Integer> SHARD_MOD_REMAINDER =
            key("shardModRemainder".toLowerCase())
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "SLS shard mod remainder. Using it with shard mod divisor to read part of shards for a logstore.");

    public static final ConfigOption<String> METADATA_LIST =
            key("metadata.list".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of readable metadata from ConsumerRecord to be passed to downstream, split by `,`. "
                                    + "Available readable metadata are: __source__, __topic__, __timestamp__.");
}
