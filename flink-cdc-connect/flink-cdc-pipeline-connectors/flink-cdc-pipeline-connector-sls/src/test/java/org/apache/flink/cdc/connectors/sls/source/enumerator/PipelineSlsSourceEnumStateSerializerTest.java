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
import org.apache.flink.cdc.common.types.DataTypes;

import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link PipelineSlsSourceEnumStateSerializer}. */
public class PipelineSlsSourceEnumStateSerializerTest {

    private static final TableId TABLE_ID_1 = TableId.tableId("test_project", "test_logstore");
    private static final Schema SCHEMA_1 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final TableId TABLE_ID_2 = TableId.tableId("inventory", "products");
    private static final Schema SCHEMA_2 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("weight", DataTypes.DOUBLE())
                    .build();

    @Test
    public void testEnumStateSerde() throws Exception {
        Map<TableId, Schema> tableSchemas = new HashMap<>();
        tableSchemas.put(TABLE_ID_1, SCHEMA_1);
        tableSchemas.put(TABLE_ID_2, SCHEMA_2);

        Set<LogStoreShard> assignedShards = new HashSet<>();
        assignedShards.add(new LogStoreShard(0, false, "00", "01"));
        assignedShards.add(new LogStoreShard(1, false, "01", "02"));

        Map<LogStoreShard, String> finishedShards = new HashMap<>();
        finishedShards.put(new LogStoreShard(2, true, "02", "03"), "cursorForShard2");
        finishedShards.put(new LogStoreShard(3, true, "03", "04"), "cursorForShard3");

        PipelineSlsSourceEnumState state =
                new PipelineSlsSourceEnumState(
                        assignedShards, finishedShards, new HashSet<>(), true, tableSchemas, true);

        PipelineSlsSourceEnumStateSerializer serializer =
                new PipelineSlsSourceEnumStateSerializer();
        byte[] bytes = serializer.serialize(state);
        PipelineSlsSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(state.assignedShards(), restoredState.assignedShards());
        assertEquals(state.finishedShards(), restoredState.finishedShards());
        assertEquals(state.unassignedInitialShards(), restoredState.unassignedInitialShards());
        assertEquals(state.initialDiscoveryFinished(), restoredState.initialDiscoveryFinished());

        assertEquals(state.getInitialInferredSchemas(), restoredState.getInitialInferredSchemas());
        assertEquals(
                state.isInitialSchemaInferenceFinished(),
                restoredState.isInitialSchemaInferenceFinished());
    }
}
