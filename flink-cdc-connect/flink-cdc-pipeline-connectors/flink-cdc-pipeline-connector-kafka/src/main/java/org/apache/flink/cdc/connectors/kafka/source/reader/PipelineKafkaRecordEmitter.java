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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.KafkaRecordSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaAware;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplitState;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kafka.source.reader.KafkaRecordEmitter;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * The {@link RecordEmitter} implementation for {@link PipelineKafkaSourceReader}. Compared to
 * {@link KafkaRecordEmitter}, it updates table schema in {@link PipelineKafkaPartitionSplitState}
 * additionally.
 */
public class PipelineKafkaRecordEmitter
        implements RecordEmitter<ConsumerRecord<byte[], byte[]>, Event, KafkaPartitionSplitState>,
                SchemaAware {

    private final KafkaRecordSchemaAwareDeserializationSchema<Event> deserializationSchema;
    private final SourceOutputWrapper sourceOutputWrapper = new SourceOutputWrapper();

    public PipelineKafkaRecordEmitter(
            KafkaRecordDeserializationSchema<Event> deserializationSchema) {
        this.deserializationSchema =
                (KafkaRecordSchemaAwareDeserializationSchema<Event>) deserializationSchema;
    }

    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord,
            SourceOutput<Event> output,
            KafkaPartitionSplitState splitState)
            throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());
            sourceOutputWrapper.setTableId(null);
            deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);
            splitState.setCurrentOffset(consumerRecord.offset() + 1);

            PipelineKafkaPartitionSplitState pipelineSplitState =
                    (PipelineKafkaPartitionSplitState) splitState;
            if (sourceOutputWrapper.getTableId() != null) {
                TableId tableId = sourceOutputWrapper.getTableId();
                Schema keySchema = deserializationSchema.getKeyTableSchema(tableId);
                Schema schema = deserializationSchema.getTableSchema(tableId);
                if (schema == null) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Cannot update schema state for table [%s], topic partition is [%s]",
                                    tableId, splitState.getTopicPartition()));
                }
                if (keySchema != null) {
                    pipelineSplitState.setTableSchema(tableId, schema);
                }
                pipelineSplitState.setTableSchema(tableId, schema);
            }
        } catch (Exception e) {
            throw new IOException("Failed to deserialize consumer record due to", e);
        }
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return deserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        deserializationSchema.setTableSchema(tableId, schema);
    }

    @Override
    public void setKeyTableSchema(TableId tableId, Schema schema) {
        deserializationSchema.setKeyTableSchema(tableId, schema);
    }

    @Override
    public Schema getKeyTableSchema(TableId tableId) {
        return deserializationSchema.getKeyTableSchema(tableId);
    }

    private static class SourceOutputWrapper implements Collector<Event> {

        private SourceOutput<Event> sourceOutput;
        private long timestamp;
        private TableId tableId;

        @Override
        public void collect(Event record) {
            sourceOutput.collect(record, timestamp);
            if (record instanceof ChangeEvent) {
                tableId = ((ChangeEvent) record).tableId();
            }
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<Event> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setTableId(TableId tableId) {
            this.tableId = tableId;
        }

        private TableId getTableId() {
            return tableId;
        }
    }
}
