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

package org.apache.flink.cdc.connectors.kafka.source.split;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaAware;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;

import java.util.Map;

/** This class extends KafkaPartitionSplitState to track mutable table schemas. */
public class PipelineKafkaPartitionSplitState extends KafkaPartitionSplitState
        implements SchemaAware {

    private final Map<TableId, Schema> keyTableSchemas;
    private final Map<TableId, Schema> valueTableSchemas;

    public PipelineKafkaPartitionSplitState(PipelineKafkaPartitionSplit split) {
        super(split);
        this.keyTableSchemas = split.getKeyTableSchemas();
        this.valueTableSchemas = split.getValueTableSchemas();
    }

    @Override
    public KafkaPartitionSplit toKafkaPartitionSplit() {
        return new PipelineKafkaPartitionSplit(
                getTopicPartition(),
                getCurrentOffset(),
                getStoppingOffset().orElse(NO_STOPPING_OFFSET),
                valueTableSchemas,
                keyTableSchemas);
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return valueTableSchemas.get(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        valueTableSchemas.put(tableId, schema);
    }

    @Override
    public void setKeyTableSchema(TableId tableId, Schema schema) {
        keyTableSchemas.put(tableId, schema);
    }

    @Override
    public Schema getKeyTableSchema(TableId tableId) {
        return keyTableSchemas.get(tableId);
    }
}
