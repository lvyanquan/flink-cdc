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

import com.alibaba.ververica.connectors.sls.newsource.model.LogStore;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;

import java.util.Map;
import java.util.Objects;

/** A SourceSplit for an SLS shard with table schemas. */
public class PipelineSlsShardSplit extends SlsShardSplit {
    private final Map<TableId, Schema> tableSchemas;

    public PipelineSlsShardSplit(
            LogStore logStore,
            LogStoreShard shard,
            String startCursor,
            String endCursor,
            Map<TableId, Schema> tableSchemas) {
        super(logStore, shard, startCursor, endCursor);
        this.tableSchemas = tableSchemas;
    }

    public Map<TableId, Schema> getTableSchemas() {
        return tableSchemas;
    }

    @Override
    public String toString() {
        return String.format(
                "SlsShardSplit(logstore=%s, shard=%s, startingCursor=%s, stoppingCursor=%s, tableSchemas=%s)",
                getLogstore(), getShard(), getStartCursor(), getEndCursor(), tableSchemas);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof PipelineSlsShardSplit)) {
            return false;
        }
        PipelineSlsShardSplit split = (PipelineSlsShardSplit) o;
        return Objects.equals(getLogstore(), split.getLogstore())
                && Objects.equals(getShard(), split.getShard())
                && Objects.equals(getStartCursor(), split.getStartCursor())
                && Objects.equals(getEndCursor(), split.getEndCursor())
                && Objects.equals(tableSchemas, split.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getLogstore(), getShard(), getStartCursor(), getEndCursor(), tableSchemas);
    }
}
