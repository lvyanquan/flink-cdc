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

package org.apache.flink.cdc.connectors.kafka.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParser;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaAware;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaEvolveManager;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaEvolveResult;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplit;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplitState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

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

/** The source reader for Kafka partitions. */
public class PipelineKafkaSourceReader extends KafkaSourceReader<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineKafkaSourceReader.class);
    private static final long POLL_TIMEOUT = 10000L;

    private final RecordSchemaParser recordSchemaParser;
    private final int maxFetchRecords;
    private final KafkaConsumer<byte[], byte[]> consumer;

    public PipelineKafkaSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                    elementsQueue,
            KafkaSourceFetcherManager kafkaSourceFetcherManager,
            RecordEmitter<ConsumerRecord<byte[], byte[]>, Event, KafkaPartitionSplitState>
                    recordEmitter,
            Configuration config,
            SourceReaderContext context,
            KafkaSourceReaderMetrics kafkaSourceReaderMetrics,
            RecordSchemaParser recordSchemaParser,
            int maxFetchRecords,
            Properties props) {
        super(
                elementsQueue,
                kafkaSourceFetcherManager,
                recordEmitter,
                config,
                context,
                kafkaSourceReaderMetrics);
        this.recordSchemaParser = recordSchemaParser;
        this.maxFetchRecords = maxFetchRecords;
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        consumerProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG,
                createConsumerClientId(props, context.getIndexOfSubtask()));
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    @Override
    protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
        PipelineKafkaPartitionSplit pipelineKafkaPartitionSplit =
                (PipelineKafkaPartitionSplit) split;
        Map<TableId, Schema> tableSchemas = pipelineKafkaPartitionSplit.getTableSchemas();
        if (tableSchemas.isEmpty() && maxFetchRecords > 0) {
            // Partition has not been consumed, consume some records to get initial schemas
            tableSchemas =
                    getTableSchemasByParsingRecord(split.getTopicPartition(), maxFetchRecords);
        }
        LOG.info(
                "The initialized table schemas for partition {}: {}",
                split.getTopicPartition(),
                tableSchemas);
        // initialize table schemas
        tableSchemas.forEach(
                (tableId, schema) -> ((SchemaAware) recordEmitter).setTableSchema(tableId, schema));
        return new PipelineKafkaPartitionSplitState(pipelineKafkaPartitionSplit);
    }

    @Override
    protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
        PipelineKafkaPartitionSplitState pipelineKafkaPartitionSplitState =
                (PipelineKafkaPartitionSplitState) splitState;
        return pipelineKafkaPartitionSplitState.toKafkaPartitionSplit();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (consumer != null) {
            consumer.close();
        }
    }

    private String createConsumerClientId(Properties props, int subtaskId) {
        String prefix = props.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        return prefix + "-pre-consumer-" + subtaskId;
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
                    TableId tableId = tableSchemaOp.get().f0;
                    Schema schema = tableSchemaOp.get().f1;
                    if (tableSchemas.containsKey(tableId)) {
                        Schema baseSchema = tableSchemas.get(tableId);
                        SchemaEvolveResult result =
                                SchemaEvolveManager.evolveSchema(tableId, baseSchema, schema);
                        if (result.isCompatibleAsIs()) {
                            continue;
                        }
                        if (result.isCompatibleAfterEvolution()) {
                            tableSchemas.put(tableId, result.getSchemaAfterEvolve());
                        } else {
                            throw new RuntimeException(
                                    String.format(
                                            "Schemas are incompatible. %s",
                                            result.getIncompatibleReason()));
                        }
                    } else {
                        tableSchemas.put(tableId, schema);
                    }
                }
            }
            return tableSchemas;
        } catch (Exception e) {
            LOG.warn(
                    String.format("Failed to initialize table schemas for partition %s", partition),
                    e);
            return Collections.emptyMap();
        }
    }
}
