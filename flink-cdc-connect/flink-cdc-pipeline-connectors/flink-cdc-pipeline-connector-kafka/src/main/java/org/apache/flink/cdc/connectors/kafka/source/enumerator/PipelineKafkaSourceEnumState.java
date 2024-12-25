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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.TopicPartitionAndAssignmentStatus;

import java.util.Map;
import java.util.Set;

/** The state of pipeline Kafka source enumerator. */
public class PipelineKafkaSourceEnumState extends KafkaSourceEnumState {

    /** The table schemas inferred by initial schema inference. */
    private final Map<TableId, Schema> initialInferredSchemas;

    /** This flag will be marked as true if initial schema inference has finished. */
    private final boolean initialSchemaInferenceFinished;

    public PipelineKafkaSourceEnumState(
            Set<TopicPartitionAndAssignmentStatus> partitions,
            boolean initialDiscoveryFinished,
            Map<TableId, Schema> initialInferredSchemas,
            boolean initialSchemaInferenceFinished) {
        super(partitions, initialDiscoveryFinished);
        this.initialInferredSchemas = initialInferredSchemas;
        this.initialSchemaInferenceFinished = initialSchemaInferenceFinished;
    }

    public Map<TableId, Schema> getInitialInferredSchemas() {
        return initialInferredSchemas;
    }

    public boolean isInitialSchemaInferenceFinished() {
        return initialSchemaInferenceFinished;
    }
}
