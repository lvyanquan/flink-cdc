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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationType;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.source.metadata.KafkaReadableMetadata;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParser;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParserFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.BoundedOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.StartupOptions;

import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.inference.SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY;
import static org.apache.flink.cdc.common.utils.OptionUtils.VVR_START_TIME_MS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.KEY_FIELDS_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.KEY_FORMAT;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.PROPS_GROUP_ID;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_BOUNDED_MODE;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_BOUNDED_SPECIFIC_OFFSETS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_CHECK_DUPLICATED_GROUP_ID;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.TOPIC;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.TOPIC_PATTERN;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.VALUE_FIELDS_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getBoundedOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getTopicPattern;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getTopics;

/** A {@link DataSourceFactory} to create {@link KafkaDataSource}. */
public class KafkaDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSource createDataSource(Context context) {
        SchemaInferenceStrategy schemaInferenceStrategy =
                context.getFactoryConfiguration().get(SCHEMA_INFERENCE_STRATEGY);
        int maxFetchRecords = context.getFactoryConfiguration().get(SCAN_MAX_PRE_FETCH_RECORDS);
        if (schemaInferenceStrategy == SchemaInferenceStrategy.STATIC && maxFetchRecords <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be greater than 0 if schema inference strategy is static.",
                            SCAN_MAX_PRE_FETCH_RECORDS.key()));
        }

        @Nullable
        JsonDeserializationType keyJsonDeserializationType =
                context.getFactoryConfiguration().get(KEY_FORMAT);
        JsonDeserializationType valueJsonDeserializationType =
                context.getFactoryConfiguration().get(VALUE_FORMAT);

        FactoryHelper helper = FactoryHelper.createFactoryHelper(this, context);
        if (keyJsonDeserializationType != null) {
            helper.validateExcept(
                    PROPERTIES_PREFIX,
                    keyJsonDeserializationType.toString(),
                    valueJsonDeserializationType.toString());
        } else {
            helper.validateExcept(PROPERTIES_PREFIX, valueJsonDeserializationType.toString());
        }

        Configuration configuration =
                Configuration.fromMap(context.getFactoryConfiguration().toMap());

        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }

        SchemaAwareDeserializationSchema<Event> keyDeserialization = null;
        RecordSchemaParser keyRecordSchemaParser = null;
        if (keyJsonDeserializationType != null) {
            org.apache.flink.cdc.common.configuration.Configuration keyFormatConfig =
                    helper.getFormatConfig(keyJsonDeserializationType.toString());
            keyDeserialization =
                    ChangeLogJsonFormatFactory.createDeserializationSchema(
                            SchemaInferenceStrategy.STATIC,
                            keyFormatConfig,
                            keyJsonDeserializationType,
                            zoneId);
            keyRecordSchemaParser =
                    RecordSchemaParserFactory.createRecordSchemaParser(
                            keyFormatConfig, keyJsonDeserializationType, zoneId);
        }

        org.apache.flink.cdc.common.configuration.Configuration formatConfig =
                helper.getFormatConfig(valueJsonDeserializationType.toString());
        SchemaAwareDeserializationSchema<Event> valueDeserialization =
                ChangeLogJsonFormatFactory.createDeserializationSchema(
                        schemaInferenceStrategy,
                        formatConfig,
                        valueJsonDeserializationType,
                        zoneId);
        RecordSchemaParser valueRecordSchemaParser =
                RecordSchemaParserFactory.createRecordSchemaParser(
                        formatConfig, valueJsonDeserializationType, zoneId);

        final Properties kafkaProperties = new Properties();
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        allOptions.keySet().stream()
                .filter(key -> key.startsWith(KafkaDataSourceOptions.PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = allOptions.get(key);
                            final String subKey =
                                    key.substring(
                                            (KafkaDataSourceOptions.PROPERTIES_PREFIX).length());
                            kafkaProperties.put(subKey, value);
                        });
        checkKafkaProperties(kafkaProperties);

        // add topic-partition discovery
        final Duration partitionDiscoveryInterval =
                context.getFactoryConfiguration().get(SCAN_TOPIC_PARTITION_DISCOVERY);
        kafkaProperties.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                Long.toString(partitionDiscoveryInterval.toMillis()));

        // whether to check duplicated group ID
        kafkaProperties.setProperty(
                KafkaSourceOptions.CHECK_DUPLICATED_GROUP_ID.key(),
                context.getFactoryConfiguration().get(SCAN_CHECK_DUPLICATED_GROUP_ID).toString());

        final StartupOptions startupOptions = getStartupOptions(configuration);
        final BoundedOptions boundedOptions = getBoundedOptions(configuration);

        String metadataList = context.getFactoryConfiguration().get(METADATA_LIST);
        List<KafkaReadableMetadata> readableMetadataList = listReadableMetadata(metadataList);

        @Nullable String keyPrefix = context.getFactoryConfiguration().get(KEY_FIELDS_PREFIX);
        @Nullable String valuePrefix = context.getFactoryConfiguration().get(VALUE_FIELDS_PREFIX);

        return new KafkaDataSource(
                keyDeserialization,
                keyRecordSchemaParser,
                schemaInferenceStrategy,
                valueDeserialization,
                valueRecordSchemaParser,
                maxFetchRecords,
                isParallelMetadataSource(formatConfig, valueJsonDeserializationType),
                getTopics(configuration),
                getTopicPattern(configuration),
                kafkaProperties,
                startupOptions.startupMode,
                startupOptions.specificOffsets,
                startupOptions.startupTimestampMillis,
                boundedOptions.boundedMode,
                boundedOptions.specificOffsets,
                boundedOptions.boundedTimestampMillis,
                readableMetadataList,
                keyPrefix,
                valuePrefix);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEMA_INFERENCE_STRATEGY);
        options.add(KEY_FORMAT);
        options.add(VALUE_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FIELDS_PREFIX);
        options.add(TOPIC);
        options.add(TOPIC_PATTERN);
        options.add(PROPS_GROUP_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_BOUNDED_MODE);
        options.add(SCAN_BOUNDED_SPECIFIC_OFFSETS);
        options.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
        options.add(SCAN_CHECK_DUPLICATED_GROUP_ID);
        options.add(SCAN_MAX_PRE_FETCH_RECORDS);
        options.add(VVR_START_TIME_MS);
        options.add(METADATA_LIST);
        return options;
    }

    private void checkKafkaProperties(Properties kafkaProperties) {
        if (kafkaProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s%s must be set for kafka.",
                            PROPERTIES_PREFIX, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }
    }

    private boolean isParallelMetadataSource(
            org.apache.flink.cdc.common.configuration.Configuration formatOptions,
            JsonDeserializationType formatType) {
        switch (formatType) {
            case DEBEZIUM_JSON:
                return formatOptions.get(DebeziumJsonFormatOptions.DISTRIBUTED_TABLES);
            case CANAL_JSON:
                return formatOptions.get(CanalJsonFormatOptions.DISTRIBUTED_TABLES);
            case JSON:
                return true;
            default:
                throw new IllegalArgumentException(
                        "UnSupport JsonDeserializationType of " + formatType);
        }
    }

    private List<KafkaReadableMetadata> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return Collections.emptyList();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<KafkaReadableMetadata> foundMetadata = new ArrayList<>();
        for (KafkaReadableMetadata metadata : KafkaReadableMetadata.values()) {
            if (readableMetadataList.contains(metadata.getKey())) {
                foundMetadata.add(metadata);
                readableMetadataList.remove(metadata.getKey());
            }
        }
        if (readableMetadataList.isEmpty()) {
            return foundMetadata;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] cannot be found in kafka metadata.",
                        String.join(", ", readableMetadataList)));
    }
}
