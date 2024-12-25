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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.connectors.kafka.source.enumerator.PipelineKafkaSourceEnumState;
import org.apache.flink.cdc.connectors.kafka.source.enumerator.PipelineKafkaSourceEnumStateSerializer;
import org.apache.flink.cdc.connectors.kafka.source.enumerator.PipelineKafkaSourceEnumerator;
import org.apache.flink.cdc.connectors.kafka.source.reader.PipelineKafkaRecordEmitter;
import org.apache.flink.cdc.connectors.kafka.source.reader.PipelineKafkaSourceReader;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParser;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplitSerializer;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaPartitionSplitReader;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * The Source implementation of pipeline Kafka. Please use {@link PipelineKafkaSourceBuilder} to
 * construct a {@link PipelineKafkaSource}.
 */
public class PipelineKafkaSource extends KafkaSource<Event> {

    private final SchemaInferenceStrategy schemaInferenceStrategy;
    private final RecordSchemaParser recordSchemaParser;
    private final int maxFetchRecords;

    PipelineKafkaSource(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            KafkaRecordDeserializationSchema<Event> deserializationSchema,
            Properties props,
            SchemaInferenceStrategy schemaInferenceStrategy,
            RecordSchemaParser recordSchemaParser,
            int maxFetchRecords) {
        super(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                deserializationSchema,
                props);
        this.schemaInferenceStrategy = schemaInferenceStrategy;
        this.recordSchemaParser = recordSchemaParser;
        this.maxFetchRecords = maxFetchRecords;
    }

    public static PipelineKafkaSourceBuilder builder() {
        return new PipelineKafkaSourceBuilder();
    }

    @Override
    public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
        return new PipelineKafkaPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new PipelineKafkaSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<Event, KafkaPartitionSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                elementsQueue = new FutureCompletingBlockingQueue<>();
        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(readerContext.metricGroup());

        Supplier<KafkaPartitionSplitReader> splitReaderSupplier =
                () -> new KafkaPartitionSplitReader(props, readerContext, kafkaSourceReaderMetrics);
        PipelineKafkaRecordEmitter recordEmitter =
                new PipelineKafkaRecordEmitter(deserializationSchema);

        return new PipelineKafkaSourceReader(
                elementsQueue,
                new KafkaSourceFetcherManager(
                        elementsQueue, splitReaderSupplier::get, (ignore) -> {}),
                recordEmitter,
                toConfiguration(props),
                readerContext,
                kafkaSourceReaderMetrics);
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
            SplitEnumeratorContext<KafkaPartitionSplit> enumContext) {
        return new PipelineKafkaSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                boundedness,
                maxFetchRecords,
                recordSchemaParser);
    }

    @Override
    public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            KafkaSourceEnumState checkpoint)
            throws IOException {
        return new PipelineKafkaSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                boundedness,
                (PipelineKafkaSourceEnumState) checkpoint,
                maxFetchRecords,
                recordSchemaParser);
    }
}
