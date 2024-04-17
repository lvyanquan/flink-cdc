/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.debezium.dispatcher;

import com.lmax.disruptor.EventHandler;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Send {@link SourceRecord} to {@link EventDispatcher.StreamingChangeRecordReceiver}, this should
 * be called sequentially.
 */
public class ChangeRecordEventConsumer<T extends DataCollectionId>
        implements EventHandler<ChangeRecordEvent<T>> {

    protected final EventDispatcher<MySqlPartition, T> eventDispatcher;

    public ChangeRecordEventConsumer(EventDispatcher<MySqlPartition, T> eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    /**
     * copy from {@link RelationalChangeRecordEmitter#emitChangeRecords(DataCollectionSchema,
     * ChangeRecordEmitter.Receiver)}.
     */
    @Override
    public void onEvent(ChangeRecordEvent event, long sequence, boolean endOfBatch)
            throws InterruptedException {
        Envelope.Operation operation = event.getChangeRecordEmitter().getOperation();
        TableSchema tableSchema = event.getTableSchema();
        Struct newKey = event.getNewKey();
        Struct oldKey = event.getOldKey();

        switch (operation) {
            case CREATE:
                {
                    changeRecord(
                            event.partition,
                            tableSchema,
                            Envelope.Operation.CREATE,
                            newKey,
                            event.getCreateEnvelope(),
                            event.getChangeRecordEmitter().getOffset(),
                            null,
                            event.getDataCollectionId());
                }
                break;
            case READ:
                {
                    changeRecord(
                            event.partition,
                            tableSchema,
                            Envelope.Operation.READ,
                            newKey,
                            event.getReadEnvelope(),
                            event.getChangeRecordEmitter().getOffset(),
                            null,
                            event.getDataCollectionId());
                }
                break;
            case UPDATE:
                {
                    if (event.getUpdateEnvelope() == null) {
                        ConnectHeaders headers = new ConnectHeaders();
                        headers.add(
                                RelationalChangeRecordEmitter.PK_UPDATE_NEWKEY_FIELD,
                                newKey,
                                tableSchema.keySchema());
                        changeRecord(
                                event.partition,
                                tableSchema,
                                Envelope.Operation.DELETE,
                                oldKey,
                                event.getDeleteEnvelope(),
                                event.getChangeRecordEmitter().getOffset(),
                                headers,
                                event.dataCollectionId);
                        headers = new ConnectHeaders();
                        headers.add(
                                RelationalChangeRecordEmitter.PK_UPDATE_OLDKEY_FIELD,
                                oldKey,
                                tableSchema.keySchema());
                        changeRecord(
                                event.partition,
                                tableSchema,
                                Envelope.Operation.CREATE,
                                newKey,
                                event.getCreateEnvelope(),
                                event.getChangeRecordEmitter().getOffset(),
                                headers,
                                event.dataCollectionId);
                    } else {
                        changeRecord(
                                event.partition,
                                tableSchema,
                                Envelope.Operation.UPDATE,
                                newKey,
                                event.getUpdateEnvelope(),
                                event.getChangeRecordEmitter().getOffset(),
                                null,
                                event.dataCollectionId);
                    }
                }
                break;
            case DELETE:
                {
                    changeRecord(
                            event.partition,
                            tableSchema,
                            Envelope.Operation.DELETE,
                            oldKey,
                            event.getDeleteEnvelope(),
                            event.getChangeRecordEmitter().getOffset(),
                            null,
                            event.getDataCollectionId());
                }
                break;
            case TRUNCATE:
                {
                    throw new UnsupportedOperationException("TRUNCATE not supported");
                }
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
        event.clearAll();
    }

    /**
     * copy from line 311~356 of {@link EventDispatcher#dispatchDataChangeEvent(Partition,
     * DataCollectionId, ChangeRecordEmitter)}.
     */
    private void changeRecord(
            MySqlPartition partition,
            DataCollectionSchema schema,
            Envelope.Operation operation,
            Object key,
            Struct value,
            OffsetContext offset,
            ConnectHeaders headers,
            DataCollectionId dataCollectionId)
            throws InterruptedException {
        if (operation == Envelope.Operation.CREATE
                && eventDispatcher.signal.isSignal(dataCollectionId)) {
            eventDispatcher.signal.process(partition, value, offset);
        }

        if (eventDispatcher.neverSkip || !eventDispatcher.skippedOperations.contains(operation)) {
            eventDispatcher.transactionMonitor.dataEvent(
                    partition, dataCollectionId, offset, key, value);
            eventDispatcher.eventListener.onEvent(
                    partition, dataCollectionId, offset, key, value, operation);
            if (eventDispatcher.incrementalSnapshotChangeEventSource != null) {
                eventDispatcher.incrementalSnapshotChangeEventSource.processMessage(
                        partition, dataCollectionId, key, offset);
            }
            eventDispatcher.streamingReceiver.changeRecord(
                    partition, schema, operation, key, value, offset, headers);
        }

        eventDispatcher.heartbeat.heartbeat(
                partition.getSourcePartition(),
                offset.getOffset(),
                eventDispatcher::enqueueHeartbeat);
    }
}
