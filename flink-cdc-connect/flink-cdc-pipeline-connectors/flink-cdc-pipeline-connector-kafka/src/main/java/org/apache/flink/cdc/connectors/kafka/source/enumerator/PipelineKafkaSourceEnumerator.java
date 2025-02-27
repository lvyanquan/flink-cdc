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

package org.apache.flink.cdc.connectors.kafka.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.kafka.source.PipelineKafkaSource;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParser;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/** The enumerator class for {@link PipelineKafkaSource}. */
public class PipelineKafkaSourceEnumerator extends KafkaSourceEnumerator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineKafkaSourceEnumerator.class);
    private static final long POLL_TIMEOUT = 10000L;

    private final int maxFetchRecords;
    @Nullable private final RecordSchemaParser recordKeySchemaParser;
    private final RecordSchemaParser recordValueSchemaParser;

    /**
     * The initial inferred table schemas for key and value respectively. It will never change after
     * initializing.
     */
    private final Map<TableId, Schema> initialInferredValueSchemas;

    private final Map<TableId, Schema> initialInferredKeySchemas;

    private final Properties properties;

    private boolean initialSchemaInferenceFinished;
    private KafkaConsumer<byte[], byte[]> consumer;

    public PipelineKafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            int maxFetchRecords,
            @Nullable RecordSchemaParser recordKeySchemaParser,
            RecordSchemaParser recordValueSchemaParser) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness);
        this.maxFetchRecords = maxFetchRecords;
        this.recordKeySchemaParser = recordKeySchemaParser;
        this.recordValueSchemaParser = recordValueSchemaParser;
        this.properties = properties;

        this.initialInferredValueSchemas = new HashMap<>();
        this.initialInferredKeySchemas = new HashMap<>();
    }

    public PipelineKafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            PipelineKafkaSourceEnumState kafkaSourceEnumState,
            int maxFetchRecords,
            @Nullable RecordSchemaParser recordKeySchemaParser,
            RecordSchemaParser recordValueSchemaParser) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                kafkaSourceEnumState);
        this.maxFetchRecords = maxFetchRecords;
        this.recordKeySchemaParser = recordKeySchemaParser;
        this.recordValueSchemaParser = recordValueSchemaParser;
        this.properties = properties;

        this.initialInferredKeySchemas = kafkaSourceEnumState.getInitialInferredKeySchemas();
        this.initialInferredValueSchemas = kafkaSourceEnumState.getInitialInferredValueSchemas();
        this.initialSchemaInferenceFinished =
                kafkaSourceEnumState.isInitialSchemaInferenceFinished();
    }

    @Override
    public void start() {
        try {
            if (recordKeySchemaParser != null) {
                recordKeySchemaParser.open();
            }
            recordValueSchemaParser.open();
        } catch (Exception e) {
            throw new IllegalStateException("Open recordSchemaParser failed.", e);
        }
        consumer = getKafkaConsumer();
        super.start();
    }

    @Override
    public KafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        KafkaSourceEnumState state = super.snapshotState(checkpointId);
        // add state about initial inferred schemas in enumerator state
        return new PipelineKafkaSourceEnumState(
                state.partitions(),
                state.initialDiscoveryFinished(),
                initialInferredKeySchemas,
                initialInferredValueSchemas,
                initialSchemaInferenceFinished);
    }

    @Override
    protected PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        if (!initialSchemaInferenceFinished) {
            // fetch records per partition to initialize the inferred schemas
            Map<TableId, Schema> initialValueTableSchemas = new HashMap<>();
            Map<TableId, Schema> initialKeyTableSchemas = new HashMap<>();
            partitionChange
                    .getNewPartitions()
                    .forEach(
                            partition -> {
                                Tuple2<Map<TableId, Schema>, Map<TableId, Schema>> tableSchemas =
                                        getTableSchemasByParsingRecord(partition, maxFetchRecords);
                                tableSchemas.f0.forEach(
                                        (tableId, schema) ->
                                                initialKeyTableSchemas.put(
                                                        tableId,
                                                        SchemaMergingUtils.getLeastCommonSchema(
                                                                initialKeyTableSchemas.get(tableId),
                                                                schema)));
                                tableSchemas.f1.forEach(
                                        (tableId, schema) ->
                                                initialValueTableSchemas.put(
                                                        tableId,
                                                        SchemaMergingUtils.getLeastCommonSchema(
                                                                initialValueTableSchemas.get(
                                                                        tableId),
                                                                schema)));
                            });
            if (maxFetchRecords > 0 && initialValueTableSchemas.isEmpty()) {
                String errorMessage =
                        String.format(
                                "Cannot infer initial table schemas for partitions: <%s>, it may caused by dirty data or empty partitions.",
                                partitionChange.getNewPartitions());
                LOG.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            this.initialInferredKeySchemas.putAll(initialKeyTableSchemas);
            this.initialInferredValueSchemas.putAll(initialValueTableSchemas);
            this.initialSchemaInferenceFinished = true;
            LOG.info(
                    "The initial inferred key table schemas: {}, value schemas: {}",
                    initialKeyTableSchemas,
                    initialValueTableSchemas);
        }
        return super.initializePartitionSplits(partitionChange);
    }

    @Override
    public void close() {
        super.close();
        consumer.close();
    }

    @Override
    protected KafkaPartitionSplit createKafkaPartitionSplit(
            TopicPartition tp, long startingOffset, long stoppingOffset) {
        // attach initial inferred table schemas to splits
        return new PipelineKafkaPartitionSplit(
                tp,
                startingOffset,
                stoppingOffset,
                initialInferredValueSchemas,
                initialInferredKeySchemas);
    }

    private KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.putAll(properties);
        consumerProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, createConsumerClientId(properties));
        return new KafkaConsumer<>(consumerProps);
    }

    private String createConsumerClientId(Properties props) {
        String prefix = props.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        return prefix + "-enumerator-pre-consumer";
    }

    private Tuple2<Map<TableId, Schema>, Map<TableId, Schema>> getTableSchemasByParsingRecord(
            TopicPartition partition, long fetchSize) {
        try {
            long beginningOffset =
                    consumer.beginningOffsets(Collections.singleton(partition))
                            .values()
                            .iterator()
                            .next();
            long endOffset =
                    consumer.endOffsets(Collections.singleton(partition))
                            .values()
                            .iterator()
                            .next();
            long partitionRecordSize = endOffset - beginningOffset;
            if (partitionRecordSize < fetchSize) {
                LOG.warn(
                        "There are only {} records in partition {}, less than the configured max fetch size {}",
                        partitionRecordSize,
                        partition,
                        fetchSize);
                fetchSize = partitionRecordSize;
            }
            if (fetchSize == 0) {
                return Tuple2.of(Collections.emptyMap(), Collections.emptyMap());
            }

            Map<TableId, Schema> keyTableSchemas = new HashMap<>();
            Map<TableId, Schema> valueTableSchemas = new HashMap<>();

            consumer.assign(Collections.singleton(partition));
            consumer.seek(partition, endOffset - fetchSize);

            int receivedRecordNum = 0;
            int parseSchemaRecordNum = 0;
            while (receivedRecordNum < fetchSize) {
                ConsumerRecords<byte[], byte[]> consumerRecords =
                        consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
                receivedRecordNum += consumerRecords.count();
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    if (recordKeySchemaParser != null) {
                        Optional<Tuple2<TableId, Schema>> keyTableSchemaOp =
                                recordKeySchemaParser.parseRecordKeySchema(record);
                        if (keyTableSchemaOp.isPresent()) {
                            TableId tableId = keyTableSchemaOp.get().f0;
                            Schema schema = keyTableSchemaOp.get().f1;
                            keyTableSchemas.put(
                                    tableId,
                                    SchemaMergingUtils.getLeastCommonSchema(
                                            keyTableSchemas.get(tableId), schema));
                        }
                    }

                    Optional<Tuple2<TableId, Schema>> valueTableSchemaOp =
                            recordValueSchemaParser.parseRecordValueSchema(record);
                    if (!valueTableSchemaOp.isPresent()) {
                        continue;
                    }
                    parseSchemaRecordNum++;
                    TableId tableId = valueTableSchemaOp.get().f0;
                    Schema schema = valueTableSchemaOp.get().f1;
                    valueTableSchemas.put(
                            tableId,
                            SchemaMergingUtils.getLeastCommonSchema(
                                    valueTableSchemas.get(tableId), schema));
                }
            }
            LOG.info(
                    "Partition {}: scan {} records for schema initialization, {} of them can parse schemas.",
                    partition,
                    receivedRecordNum,
                    parseSchemaRecordNum);
            return Tuple2.of(keyTableSchemas, valueTableSchemas);
        } catch (Exception e) {
            LOG.warn(
                    String.format("Failed to initialize table schemas for partition %s", partition),
                    e);
            return Tuple2.of(Collections.emptyMap(), Collections.emptyMap());
        }
    }
}
