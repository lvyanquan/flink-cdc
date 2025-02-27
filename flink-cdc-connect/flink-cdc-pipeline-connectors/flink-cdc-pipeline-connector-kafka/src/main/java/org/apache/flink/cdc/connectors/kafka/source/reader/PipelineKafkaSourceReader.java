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
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplit;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplitState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.connector.kafka.source.reader.fetcher.KafkaSourceFetcherManager;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The source reader for Kafka partitions. */
public class PipelineKafkaSourceReader extends KafkaSourceReader<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineKafkaSourceReader.class);

    public PipelineKafkaSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>>
                    elementsQueue,
            KafkaSourceFetcherManager kafkaSourceFetcherManager,
            RecordEmitter<ConsumerRecord<byte[], byte[]>, Event, KafkaPartitionSplitState>
                    recordEmitter,
            Configuration config,
            SourceReaderContext context,
            KafkaSourceReaderMetrics kafkaSourceReaderMetrics) {
        super(
                elementsQueue,
                kafkaSourceFetcherManager,
                recordEmitter,
                config,
                context,
                kafkaSourceReaderMetrics);
    }

    @Override
    protected KafkaPartitionSplitState initializedState(KafkaPartitionSplit split) {
        PipelineKafkaPartitionSplit pipelineKafkaPartitionSplit =
                (PipelineKafkaPartitionSplit) split;
        Map<TableId, Schema> keyTableSchemas = pipelineKafkaPartitionSplit.getKeyTableSchemas();
        Map<TableId, Schema> valueTableSchemas = pipelineKafkaPartitionSplit.getValueTableSchemas();

        LOG.info(
                "The initial table schemas for partition {} including key schemas: {}, values schemas: {}",
                split.getTopicPartition(),
                keyTableSchemas,
                valueTableSchemas);
        PipelineKafkaRecordEmitter pipelineRecordEmitter =
                (PipelineKafkaRecordEmitter) recordEmitter;
        // initialize table schemas
        keyTableSchemas.forEach(pipelineRecordEmitter::setKeyTableSchema);
        valueTableSchemas.forEach(pipelineRecordEmitter::setTableSchema);
        return new PipelineKafkaPartitionSplitState(pipelineKafkaPartitionSplit);
    }

    @Override
    protected KafkaPartitionSplit toSplitType(String splitId, KafkaPartitionSplitState splitState) {
        PipelineKafkaPartitionSplitState pipelineKafkaPartitionSplitState =
                (PipelineKafkaPartitionSplitState) splitState;
        return pipelineKafkaPartitionSplitState.toKafkaPartitionSplit();
    }
}
