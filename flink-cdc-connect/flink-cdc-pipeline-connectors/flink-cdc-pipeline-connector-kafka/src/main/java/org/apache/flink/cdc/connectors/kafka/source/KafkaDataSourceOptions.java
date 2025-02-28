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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationType;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanBoundedMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Options for {@link KafkaDataSource}. */
public class KafkaDataSourceOptions {

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    public static final ConfigOption<JsonDeserializationType> KEY_FORMAT =
            key("key.format")
                    .enumType(JsonDeserializationType.class)
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data, "
                                    + "available options are `debezium-json` and `canal-json`, default option is `debezium-json`.");

    public static final ConfigOption<JsonDeserializationType> VALUE_FORMAT =
            key("value.format")
                    .enumType(JsonDeserializationType.class)
                    .defaultValue(JsonDeserializationType.DEBEZIUM_JSON)
                    .withDescription(
                            "Defines the format identifier for encoding value data, "
                                    + "available options are `debezium-json` and `canal-json`, default option is `debezium-json`.");

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom prefix for all fields of the key format to avoid name clashes with fields of the value format. By default, the prefix is empty.");

    public static final ConfigOption<String> VALUE_FIELDS_PREFIX =
            key("value.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom prefix for all fields of the value format to avoid name clashes with fields of the key format. By default, the prefix is empty.");

    public static final ConfigOption<List<String>> TOPIC =
            key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Optional. Topic names from which the source reads.");

    public static final ConfigOption<String> TOPIC_PATTERN =
            key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional topic pattern from which the source reads. Either 'topic' or 'topic-pattern' must be set.");

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS =
            key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Kafka server connection string");

    public static final ConfigOption<String> PROPS_GROUP_ID =
            key("properties.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer group in Kafka consumer, no need for Kafka producer");

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .defaultValue(ScanStartupMode.GROUP_OFFSETS)
                    .withDescription("Startup mode for Kafka consumer.");

    public static final ConfigOption<KafkaConnectorOptions.ScanBoundedMode> SCAN_BOUNDED_MODE =
            key("scan.bounded.mode")
                    .enumType(ScanBoundedMode.class)
                    .defaultValue(ScanBoundedMode.UNBOUNDED)
                    .withDescription("Bounded mode for Kafka consumer.");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<String> SCAN_BOUNDED_SPECIFIC_OFFSETS =
            key("scan.bounded.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" bounded mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Long> SCAN_BOUNDED_TIMESTAMP_MILLIS =
            key("scan.bounded.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" bounded mode");

    public static final ConfigOption<Duration> SCAN_TOPIC_PARTITION_DISCOVERY =
            key("scan.topic-partition-discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "Optional interval for consumer to discover dynamically created Kafka partitions periodically."
                                    + "The value 0 disables the partition discovery."
                                    + "The default value is 5 minutes, which is equal to the default value of metadata.max.age.ms in Kafka.");

    public static final ConfigOption<Boolean> SCAN_CHECK_DUPLICATED_GROUP_ID =
            key("scan.check.duplicated.group.id")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to check if the consumer group ID already exists on broker. "
                                    + "Kafka source will throw an exception for duplicated group "
                                    + "ID if this option is enabled.");

    public static final ConfigOption<Integer> SCAN_MAX_PRE_FETCH_RECORDS =
            key("scan.max.pre.fetch.records")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The max number of records to be parsed for one kafka topic partition in order to get the initial table schemas.");

    public static final ConfigOption<String> METADATA_LIST =
            ConfigOptions.key("metadata.list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of readable metadata from ConsumerRecord to be passed to downstream, split by `,`. "
                                    + "Available readable metadata are: topic,partition,offset,timestamp,headers.");
}
