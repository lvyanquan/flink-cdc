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

package org.apache.flink.cdc.connectors.sls.source.split;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import com.alibaba.ververica.connectors.sls.newsource.model.LogStore;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link PipelineSlsShardSplitSerializer}. */
public class PipelineSlsShardSplitSerializerTest {

    private final Schema schema1 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .build();

    private final Schema schema2 =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("array", DataTypes.ARRAY(DataTypes.DOUBLE()))
                    .metadataColumn("meta", DataTypes.TIMESTAMP())
                    .primaryKey("id", "name")
                    .partitionKey("id")
                    .comment("comment for schema2")
                    .options(
                            new HashMap<String, String>() {
                                {
                                    put("option1", "value1");
                                    put("option2", "value2");
                                }
                            })
                    .build();

    @Test
    public void testSerializer() throws Exception {
        LogStore logStore = new LogStore("testProject", "testLogstore");
        LogStoreShard shard = new LogStoreShard(1, false, "0000", "ffff");

        Map<TableId, Schema> tableSchemas = new HashMap<>();
        tableSchemas.put(TableId.tableId("test-db1", "test-table1"), schema1);
        tableSchemas.put(TableId.tableId("test-db2", "test-table2"), schema2);

        PipelineSlsShardSplit split =
                new PipelineSlsShardSplit(logStore, shard, "start", "end", tableSchemas);

        PipelineSlsShardSplitSerializer serializer = new PipelineSlsShardSplitSerializer();
        byte[] bytes = serializer.serialize(split);
        PipelineSlsShardSplit restoredSplit =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(split, restoredSplit);
    }
}
