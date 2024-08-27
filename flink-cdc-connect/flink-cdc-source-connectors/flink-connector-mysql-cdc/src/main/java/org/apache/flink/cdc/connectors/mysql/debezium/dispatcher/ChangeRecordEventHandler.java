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

package org.apache.flink.cdc.connectors.mysql.debezium.dispatcher;

import com.lmax.disruptor.WorkHandler;
import io.debezium.connector.mysql.MySqlChangeRecordEmitter;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import org.apache.kafka.connect.data.Struct;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class that helps {@link ChangeRecordEventConsumer} complete time-consuming conversion of {@link
 * MySqlChangeRecordEmitter}.
 */
public class ChangeRecordEventHandler<T extends DataCollectionId>
        implements WorkHandler<ChangeRecordEvent<T>> {

    private final AtomicReference<Throwable> disruptorException;

    public ChangeRecordEventHandler(AtomicReference<Throwable> disruptorException) {
        this.disruptorException = disruptorException;
    }

    /**
     * The core logical copied from {@link
     * RelationalChangeRecordEmitter#emitChangeRecords(DataCollectionSchema,
     * ChangeRecordEmitter.Receiver)}.
     */
    @Override
    public void onEvent(ChangeRecordEvent<T> changeRecordEvent) {
        try {
            TableSchema tableSchema = changeRecordEvent.getTableSchema();
            MySqlChangeRecordEmitter changeRecordEmitter =
                    changeRecordEvent.getChangeRecordEmitter();
            Object[] oldColumnValues = changeRecordEmitter.getOldColumnValues();
            Object[] newColumnValues = changeRecordEmitter.getNewColumnValues();

            // tableSchema.keyFromColumnData is time-consuming conversion.
            if (oldColumnValues != null) {
                changeRecordEvent.setOldKey(tableSchema.keyFromColumnData(oldColumnValues));
            }
            if (newColumnValues != null) {
                changeRecordEvent.setNewKey(tableSchema.keyFromColumnData(newColumnValues));
            }

            // tableSchema.valueFromColumnData is time-consuming conversion.
            switch (changeRecordEmitter.getOperation()) {
                case CREATE:
                    {
                        Struct envelope =
                                tableSchema
                                        .getEnvelopeSchema()
                                        .create(
                                                tableSchema.valueFromColumnData(newColumnValues),
                                                changeRecordEvent.getSourceInfo(),
                                                changeRecordEmitter
                                                        .getClock()
                                                        .currentTimeAsInstant());
                        changeRecordEvent.setCreateEnvelope(envelope);
                    }
                    break;
                case READ:
                    {
                        Struct envelope =
                                tableSchema
                                        .getEnvelopeSchema()
                                        .read(
                                                tableSchema.valueFromColumnData(newColumnValues),
                                                changeRecordEvent.getSourceInfo(),
                                                changeRecordEmitter
                                                        .getClock()
                                                        .currentTimeAsInstant());
                        changeRecordEvent.setReadEnvelope(envelope);
                    }
                    break;
                case UPDATE:
                    {
                        if (changeRecordEvent.getOldKey() != null
                                && !Objects.equals(
                                        changeRecordEvent.getOldKey(),
                                        changeRecordEvent.getNewKey())) {
                            Struct envelope =
                                    tableSchema
                                            .getEnvelopeSchema()
                                            .delete(
                                                    tableSchema.valueFromColumnData(
                                                            oldColumnValues),
                                                    changeRecordEvent.getSourceInfo(),
                                                    changeRecordEmitter
                                                            .getClock()
                                                            .currentTimeAsInstant());
                            changeRecordEvent.setDeleteEnvelope(envelope);
                            envelope =
                                    tableSchema
                                            .getEnvelopeSchema()
                                            .create(
                                                    tableSchema.valueFromColumnData(
                                                            newColumnValues),
                                                    changeRecordEvent.getSourceInfo(),
                                                    changeRecordEmitter
                                                            .getClock()
                                                            .currentTimeAsInstant());
                            changeRecordEvent.setCreateEnvelope(envelope);
                        } else {
                            Struct envelope =
                                    tableSchema
                                            .getEnvelopeSchema()
                                            .update(
                                                    tableSchema.valueFromColumnData(
                                                            oldColumnValues),
                                                    tableSchema.valueFromColumnData(
                                                            newColumnValues),
                                                    changeRecordEvent.getSourceInfo(),
                                                    changeRecordEmitter
                                                            .getClock()
                                                            .currentTimeAsInstant());
                            changeRecordEvent.setUpdateEnvelope(envelope);
                        }
                    }
                    break;
                case DELETE:
                    {
                        Struct envelope =
                                tableSchema
                                        .getEnvelopeSchema()
                                        .delete(
                                                tableSchema.valueFromColumnData(oldColumnValues),
                                                changeRecordEvent.getSourceInfo(),
                                                changeRecordEmitter
                                                        .getClock()
                                                        .currentTimeAsInstant());
                        changeRecordEvent.setDeleteEnvelope(envelope);
                    }
                    break;
                case TRUNCATE:
                    {
                        throw new UnsupportedOperationException("TRUNCATE not supported");
                    }
                default:
                    throw new IllegalArgumentException(
                            "Unsupported operation: " + changeRecordEmitter.getOperation());
            }
        } catch (Exception e) {
            disruptorException.set(e);
        }
    }
}
