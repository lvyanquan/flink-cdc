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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.serializer.event.EventSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Operator for processing events from upstream before flowing to {@link SchemaOperator}. */
@Internal
public class DistributedPrePartitionOperator extends AbstractStreamOperator<PartitioningEvent>
        implements OneInputStreamOperator<Event, PartitioningEvent>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int downstreamParallelism;
    private final HashFunctionProvider<DataChangeEvent> hashFunctionProvider;

    // Schema and HashFunctionMap used in schema inferencing mode.
    private transient Map<TableId, Schema> schemaMap;
    private transient Map<TableId, HashFunction<DataChangeEvent>> hashFunctionMap;

    private final OperatorID schemaOperatorId;

    private transient SchemaEvolutionClient schemaEvolutionClient;

    private transient int subTaskId;

    public DistributedPrePartitionOperator(
            int downstreamParallelism,
            HashFunctionProvider<DataChangeEvent> hashFunctionProvider,
            OperatorID schemaOperatorId) {
        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.downstreamParallelism = downstreamParallelism;
        this.hashFunctionProvider = hashFunctionProvider;
        this.schemaOperatorId = schemaOperatorId;
    }

    @Override
    public void open() throws Exception {
        super.open();
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        schemaMap = new HashMap<>();
        hashFunctionMap = new HashMap<>();
        TaskOperatorEventGateway toCoordinator =
                getContainingTask().getEnvironment().getOperatorCoordinatorEventGateway();
        schemaEvolutionClient = new SchemaEvolutionClient(toCoordinator, schemaOperatorId);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();

            // Update schema map
            schemaMap.compute(
                    tableId,
                    (tId, oldSchema) ->
                            SchemaUtils.applySchemaChangeEvent(oldSchema, schemaChangeEvent));

            // Update hash function
            hashFunctionMap.put(tableId, recreateHashFunction(tableId));

            // Broadcast SchemaChangeEvent
            broadcastEvent(event);
        } else if (event instanceof DataChangeEvent) {
            // Partition DataChangeEvent by table ID and primary keys
            partitionBy((DataChangeEvent) event);
        } else {
            throw new IllegalStateException(
                    subTaskId + "> PrePartition operator received an unexpected event: " + event);
        }
    }

    private void partitionBy(DataChangeEvent dataChangeEvent) {
        output.collect(
                new StreamRecord<>(
                        PartitioningEvent.ofDistributed(
                                dataChangeEvent,
                                subTaskId,
                                hashFunctionMap
                                                .get(dataChangeEvent.tableId())
                                                .hashcode(dataChangeEvent)
                                        % downstreamParallelism)));
    }

    private void broadcastEvent(Event toBroadcast) {
        for (int i = 0; i < downstreamParallelism; i++) {
            // Deep-copying each event is required since downstream subTasks might run in the same
            // JVM
            Event copiedEvent = EventSerializer.INSTANCE.copy(toBroadcast);
            output.collect(
                    new StreamRecord<>(PartitioningEvent.ofDistributed(copiedEvent, subTaskId, i)));
        }
    }

    private String loadLatestTokenFromRegistry(TableId tableId) {
        try {
            return schemaEvolutionClient.getLatestToken(tableId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get token for " + tableId, e);
        }
    }

    private HashFunction<DataChangeEvent> recreateHashFunction(TableId tableId) {
        String token = loadLatestTokenFromRegistry(tableId);
        if (StringUtils.isNullOrWhitespaceOnly(token)) {
            return hashFunctionProvider.getHashFunction(tableId, schemaMap.get(tableId));
        } else {
            return hashFunctionProvider.getHashFunction(tableId, schemaMap.get(tableId), token);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // Needless to do anything, since AbstractStreamOperator#snapshotState and #processElement
        // is guaranteed not to be mixed together.
    }
}
