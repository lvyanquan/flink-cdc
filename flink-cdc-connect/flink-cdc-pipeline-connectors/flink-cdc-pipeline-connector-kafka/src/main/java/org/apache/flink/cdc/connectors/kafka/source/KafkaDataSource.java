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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.KafkaRecordSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A {@link DataSource} for "Kafka" connector. */
public class KafkaDataSource implements DataSource {

    private final SchemaAwareDeserializationSchema<Event> valueDeserialization;
    private final List<String> topics;
    private final Pattern topicPattern;
    private final Properties kafkaProperties;
    private final StartupMode startupMode;
    private final Map<KafkaTopicPartition, Long> specificStartupOffsets;
    private final long startupTimestampMillis;
    private final BoundedMode boundedMode;
    private final Map<KafkaTopicPartition, Long> specificBoundedOffsets;
    private final long boundedTimestampMillis;

    public KafkaDataSource(
            SchemaAwareDeserializationSchema<Event> valueDeserialization,
            List<String> topics,
            Pattern topicPattern,
            Properties kafkaProperties,
            StartupMode startupMode,
            Map<KafkaTopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis,
            BoundedMode boundedMode,
            Map<KafkaTopicPartition, Long> specificBoundedOffsets,
            long boundedTimestampMillis) {
        this.valueDeserialization = valueDeserialization;
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.kafkaProperties =
                Preconditions.checkNotNull(kafkaProperties, "Properties must not be null.");
        this.startupMode =
                Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
        this.specificStartupOffsets =
                Preconditions.checkNotNull(
                        specificStartupOffsets, "Specific offsets must not be null.");
        this.startupTimestampMillis = startupTimestampMillis;
        this.boundedMode =
                Preconditions.checkNotNull(boundedMode, "Bounded mode must not be null.");
        this.specificBoundedOffsets =
                Preconditions.checkNotNull(
                        specificBoundedOffsets, "Specific bounded offsets must not be null.");
        this.boundedTimestampMillis = boundedTimestampMillis;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        final PipelineKafkaSourceBuilder kafkaSourceBuilder = PipelineKafkaSource.builder();
        if (topics != null) {
            kafkaSourceBuilder.setTopics(topics);
        } else {
            kafkaSourceBuilder.setTopicPattern(topicPattern);
        }
        switch (startupMode) {
            case EARLIEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case LATEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        kafkaProperties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetStrategy));
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                specificStartupOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(
                                        new TopicPartition(tp.getTopic(), tp.getPartition()),
                                        offset));
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(startupTimestampMillis));
                break;
        }
        switch (boundedMode) {
            case UNBOUNDED:
                kafkaSourceBuilder.setUnbounded(new NoStoppingOffsetsInitializer());
                break;
            case LATEST:
                kafkaSourceBuilder.setBounded(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                kafkaSourceBuilder.setBounded(OffsetsInitializer.committedOffsets());
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                specificBoundedOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(
                                        new TopicPartition(tp.getTopic(), tp.getPartition()),
                                        offset));
                kafkaSourceBuilder.setBounded(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                kafkaSourceBuilder.setBounded(OffsetsInitializer.timestamp(boundedTimestampMillis));
                break;
        }

        kafkaSourceBuilder
                .setProperties(kafkaProperties)
                .setDeserializer(
                        KafkaRecordSchemaAwareDeserializationSchema.valueOnly(
                                valueDeserialization));

        return FlinkSourceProvider.of(kafkaSourceBuilder.build());
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        // this method is never used now
        throw new UnsupportedOperationException(
                "Kafka data source does not support getMetadataAccessor now.");
    }

    private OffsetResetStrategy getResetStrategy(String offsetResetConfig) {
        return Arrays.stream(OffsetResetStrategy.values())
                .filter(ors -> ors.name().equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                offsetResetConfig,
                                                Arrays.stream(OffsetResetStrategy.values())
                                                        .map(Enum::name)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }
}
