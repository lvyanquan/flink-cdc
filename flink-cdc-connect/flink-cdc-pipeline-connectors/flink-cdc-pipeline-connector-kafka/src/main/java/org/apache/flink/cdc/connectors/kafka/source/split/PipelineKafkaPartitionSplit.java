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
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/** A SourceSplit for a Kafka partition with table schemas. */
public class PipelineKafkaPartitionSplit extends KafkaPartitionSplit {

    private final Map<TableId, Schema> tableSchemas;

    public PipelineKafkaPartitionSplit(
            TopicPartition tp,
            long startingOffset,
            long stoppingOffset,
            Map<TableId, Schema> tableSchemas) {
        super(tp, startingOffset, stoppingOffset);
        this.tableSchemas = tableSchemas;
    }

    public Map<TableId, Schema> getTableSchemas() {
        return tableSchemas;
    }
}
