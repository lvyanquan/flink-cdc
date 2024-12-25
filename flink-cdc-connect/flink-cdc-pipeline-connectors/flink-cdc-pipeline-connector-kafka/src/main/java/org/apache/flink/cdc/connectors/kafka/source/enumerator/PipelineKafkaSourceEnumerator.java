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
    private final RecordSchemaParser recordSchemaParser;
    /** The initial inferred table schemas. It will never change after initializing. */
    private final Map<TableId, Schema> initialInferredSchemas;

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
            RecordSchemaParser recordSchemaParser) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness);
        this.maxFetchRecords = maxFetchRecords;
        this.recordSchemaParser = recordSchemaParser;
        this.properties = properties;

        this.initialInferredSchemas = new HashMap<>();
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
            RecordSchemaParser recordSchemaParser) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                kafkaSourceEnumState);
        this.maxFetchRecords = maxFetchRecords;
        this.recordSchemaParser = recordSchemaParser;
        this.properties = properties;

        this.initialInferredSchemas = kafkaSourceEnumState.getInitialInferredSchemas();
    }

    @Override
    public void start() {
        try {
            recordSchemaParser.open();
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
                initialInferredSchemas,
                initialSchemaInferenceFinished);
    }

    @Override
    protected PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        if (!initialSchemaInferenceFinished) {
            // fetch records per partition to initialize the inferred schemas
            Map<TableId, Schema> initialTableSchemas = new HashMap<>();
            partitionChange
                    .getNewPartitions()
                    .forEach(
                            partition -> {
                                Map<TableId, Schema> tableSchemas =
                                        getTableSchemasByParsingRecord(partition, maxFetchRecords);
                                tableSchemas.forEach(
                                        (tableId, schema) ->
                                                initialTableSchemas.put(
                                                        tableId,
                                                        SchemaMergingUtils.getLeastCommonSchema(
                                                                initialTableSchemas.get(tableId),
                                                                schema)));
                            });
            if (maxFetchRecords > 0 && initialTableSchemas.isEmpty()) {
                String errorMessage =
                        String.format(
                                "Cannot infer initial table schemas for partitions: <%s>, it may caused by dirty data or empty partitions.",
                                partitionChange.getNewPartitions());
                LOG.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            this.initialInferredSchemas.putAll(initialTableSchemas);
            this.initialSchemaInferenceFinished = true;
            LOG.info("The initial inferred table schemas: {}", initialInferredSchemas);
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
                tp, startingOffset, stoppingOffset, initialInferredSchemas);
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

    private Map<TableId, Schema> getTableSchemasByParsingRecord(
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
                return Collections.emptyMap();
            }

            Map<TableId, Schema> tableSchemas = new HashMap<>();
            consumer.assign(Collections.singleton(partition));
            consumer.seek(partition, endOffset - fetchSize);

            int receivedRecordNum = 0;
            int parseSchemaRecordNum = 0;
            while (receivedRecordNum < fetchSize) {
                ConsumerRecords<byte[], byte[]> consumerRecords =
                        consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
                receivedRecordNum += consumerRecords.count();
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    Optional<Tuple2<TableId, Schema>> tableSchemaOp =
                            recordSchemaParser.parseRecordSchema(record);
                    if (!tableSchemaOp.isPresent()) {
                        continue;
                    }
                    parseSchemaRecordNum++;
                    TableId tableId = tableSchemaOp.get().f0;
                    Schema schema = tableSchemaOp.get().f1;
                    tableSchemas.put(
                            tableId,
                            SchemaMergingUtils.getLeastCommonSchema(
                                    tableSchemas.get(tableId), schema));
                }
            }
            LOG.info(
                    "Partition {}: scan {} records for schema initialization, {} of them can parse schemas.",
                    partition,
                    receivedRecordNum,
                    parseSchemaRecordNum);
            return tableSchemas;
        } catch (Exception e) {
            LOG.warn(
                    String.format("Failed to initialize table schemas for partition %s", partition),
                    e);
            return Collections.emptyMap();
        }
    }
}
