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

package org.apache.flink.cdc.connectors.sls.source.enumerator;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import com.alibaba.ververica.connectors.sls.newsource.enumerator.SlsEnumState;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;

import java.util.Map;
import java.util.Set;

/** The state of pipeline SLS source enumerator. */
public class PipelineSlsSourceEnumState extends SlsEnumState {

    private final Map<TableId, Schema> initialInferredSchemas;
    private final boolean initialSchemaInferenceFinished;

    public PipelineSlsSourceEnumState(
            Set<LogStoreShard> assignedShards,
            Map<LogStoreShard, String> finishedShards,
            Set<LogStoreShard> unassignedInitialShards,
            boolean initialDiscoveryFinished,
            Map<TableId, Schema> initialInferredSchemas,
            boolean initialSchemaInferenceFinished) {
        super(assignedShards, finishedShards, unassignedInitialShards, initialDiscoveryFinished);
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
