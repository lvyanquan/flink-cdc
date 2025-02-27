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

package org.apache.flink.cdc.connectors.sls.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sls.source.schema.SchemaAware;
import org.apache.flink.cdc.connectors.sls.source.split.PipelineSlsShardSplit;
import org.apache.flink.cdc.connectors.sls.source.split.PipelineSlsShardSplitState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.alibaba.ververica.connectors.sls.newsource.model.SlsSourceRecord;
import com.alibaba.ververica.connectors.sls.newsource.reader.SlsSourceReader;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** The source reader for SLS shards. */
public class PipelineSlsSourceReader extends SlsSourceReader<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineSlsSourceReader.class);

    public PipelineSlsSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SlsSourceRecord>> elementsQueue,
            SplitFetcherManager<SlsSourceRecord, SlsShardSplit> splitFetcherManager,
            RecordEmitter<SlsSourceRecord, Event, SlsShardSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected SlsShardSplitState initializedState(SlsShardSplit split) {
        PipelineSlsShardSplit pipelineSlsShardSplit = (PipelineSlsShardSplit) split;
        Map<TableId, Schema> tableSchemas = pipelineSlsShardSplit.getTableSchemas();
        LOG.info(
                "The initialized table schemas for {} shard {}: {}",
                split.getLogstore(),
                split.getShard(),
                tableSchemas);
        tableSchemas.forEach(
                (tableId, schema) -> ((SchemaAware) recordEmitter).setTableSchema(tableId, schema));

        return new PipelineSlsShardSplitState(pipelineSlsShardSplit);
    }

    @Override
    protected SlsShardSplit toSplitType(String splitId, SlsShardSplitState splitState) {
        PipelineSlsShardSplitState pipelineSlsShardSplitState =
                (PipelineSlsShardSplitState) splitState;
        return pipelineSlsShardSplitState.toSplit();
    }
}
