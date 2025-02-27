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
import org.apache.flink.cdc.connectors.sls.source.schema.SchemaAware;

import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplitState;

import java.util.Map;

/** This class extends SlsShardSplitState to track mutable schema. */
public class PipelineSlsShardSplitState extends SlsShardSplitState implements SchemaAware {

    private final Map<TableId, Schema> tableSchemas;

    public PipelineSlsShardSplitState(PipelineSlsShardSplit split) {
        super(
                split.getLogstore(),
                split.getShard(),
                split.getStartCursor(),
                split.getEndCursor(),
                split.getStartCursor());
        this.tableSchemas = split.getTableSchemas();
    }

    @Override
    public SlsShardSplit toSplit() {
        return new PipelineSlsShardSplit(
                getLogstore(), getShard(), getStartCursor(), getEndCursor(), tableSchemas);
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return tableSchemas.get(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        tableSchemas.put(tableId, schema);
    }
}
