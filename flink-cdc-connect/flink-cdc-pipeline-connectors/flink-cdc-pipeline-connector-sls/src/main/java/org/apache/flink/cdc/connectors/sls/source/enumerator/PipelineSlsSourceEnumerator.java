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

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.connectors.sls.source.PipelineSlsSource;
import org.apache.flink.cdc.connectors.sls.source.split.PipelineSlsShardSplit;
import org.apache.flink.configuration.Configuration;

import com.alibaba.ververica.connectors.sls.SLSAccessInfo;
import com.alibaba.ververica.connectors.sls.SLSUtils;
import com.alibaba.ververica.connectors.sls.exception.ShardNotExistException;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.SlsEnumState;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.SlsSourceEnumerator;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.assigner.ShardAssigner;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.initializer.CursorInitializer;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStore;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.PullLogsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The enumerator class for {@link PipelineSlsSource}. */
public class PipelineSlsSourceEnumerator extends SlsSourceEnumerator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineSlsSourceEnumerator.class);
    private static final Duration FETCH_MORE_LOGS_INTERVAL = Duration.ofHours(1);
    private static final int FETCH_MORE_LOGS_RETRY_CNT = 3;

    private final int maxFetchLogGroups;
    /** The initial inferred table schemas. It will never change after initializing. */
    private final Map<TableId, Schema> initialInferredSchemas;

    private final Configuration configuration;
    private final SLSAccessInfo accessInfo;

    private boolean initialSchemaInferenceFinished;

    public PipelineSlsSourceEnumerator(
            Configuration config,
            CursorInitializer startingCursorInitializer,
            CursorInitializer stoppingCursorInitializer,
            ShardAssigner shardAssigner,
            SplitEnumeratorContext<SlsShardSplit> context,
            int maxFetchLogGroups) {
        super(config, startingCursorInitializer, stoppingCursorInitializer, shardAssigner, context);
        this.configuration = config;
        this.maxFetchLogGroups = maxFetchLogGroups;
        this.initialInferredSchemas = new HashMap<>();
        this.accessInfo = SLSUtils.parseConsumerConfig(configuration);
    }

    public PipelineSlsSourceEnumerator(
            Configuration config,
            CursorInitializer startingCursorInitializer,
            CursorInitializer stoppingCursorInitializer,
            ShardAssigner shardAssigner,
            SplitEnumeratorContext<SlsShardSplit> context,
            PipelineSlsSourceEnumState slsSourceEnumState,
            int maxFetchLogGroups) {
        super(
                config,
                startingCursorInitializer,
                stoppingCursorInitializer,
                shardAssigner,
                context,
                slsSourceEnumState);
        this.configuration = config;
        this.maxFetchLogGroups = maxFetchLogGroups;

        this.initialInferredSchemas = slsSourceEnumState.getInitialInferredSchemas();
        this.initialSchemaInferenceFinished = slsSourceEnumState.isInitialSchemaInferenceFinished();
        this.accessInfo = SLSUtils.parseConsumerConfig(configuration);
    }

    @Override
    public SlsEnumState snapshotState(long checkpointId) {
        SlsEnumState state = super.snapshotState(checkpointId);
        return new PipelineSlsSourceEnumState(
                state.assignedShards(),
                state.finishedShards(),
                state.unassignedInitialShards(),
                state.initialDiscoveryFinished(),
                initialInferredSchemas,
                initialSchemaInferenceFinished);
    }

    @Override
    protected SlsShardSplit createSlsShardSplit(
            LogStore logStore, LogStoreShard shard, String startCursor, String endCursor) {
        return new PipelineSlsShardSplit(
                logStore, shard, startCursor, endCursor, initialInferredSchemas);
    }

    @Override
    protected ShardSplitChange initializeShardSplits(ShardChange shardChange) {
        ShardSplitChange shardSplitChange = super.initializeShardSplits(shardChange);
        if (!initialSchemaInferenceFinished) {
            Schema finalSchema = null;
            if (maxFetchLogGroups > 0) {
                for (SlsShardSplit shard : shardSplitChange.newShardSplits) {
                    Schema schema = getSchemaByParsingRecord(shard);
                    finalSchema = SchemaMergingUtils.getLeastCommonSchema(finalSchema, schema);
                }
                if (finalSchema == null || finalSchema.getColumnCount() == 0) {
                    String errorMessage =
                            String.format(
                                    "Cannot infer initial table schemas for shards: <%s>, it may caused by query or empty shards.",
                                    shardChange.getNewShards());
                    LOG.error(errorMessage);
                    throw new IllegalStateException(errorMessage);
                }
            }
            if (finalSchema != null && finalSchema.getColumnCount() != 0) {
                TableId tableId =
                        TableId.tableId(accessInfo.getProjectName(), accessInfo.getLogstore());
                this.initialInferredSchemas.put(tableId, finalSchema);
            }
            this.initialSchemaInferenceFinished = true;
            LOG.info("The initial inferred table schemas: {}", initialInferredSchemas);
        }
        return shardSplitChange;
    }

    @VisibleForTesting
    Schema getSchemaByParsingRecord(SlsShardSplit shardSplit) {
        LogStoreShard shard = shardSplit.getShard();
        Set<String> columns = new LinkedHashSet<>();
        Schema.Builder builder = Schema.newBuilder();
        List<LogGroupData> logGroups;
        String cursor = shardSplit.getStartCursor();
        try {
            PullLogsResponse response =
                    client.pullData(
                            shard.getShardId(),
                            maxFetchLogGroups,
                            cursor,
                            null,
                            accessInfo.getCompressType(),
                            accessInfo.getQuery());
            logGroups = new ArrayList<>(response.getLogGroups());
            long timestamp = System.currentTimeMillis();
            int fetchCnt = 0;
            while (logGroups.size() < maxFetchLogGroups && fetchCnt < FETCH_MORE_LOGS_RETRY_CNT) {
                timestamp -= FETCH_MORE_LOGS_INTERVAL.toMillis();
                LOG.info(
                        "Fetched {} log groups for {}, try to pull logs at timestamp {}",
                        logGroups.size(),
                        shard,
                        timestamp);
                cursor = cursorRetriever.cursorForTime(shard, (int) (timestamp / 1000));
                response =
                        client.pullData(
                                shard.getShardId(),
                                maxFetchLogGroups - logGroups.size(),
                                cursor,
                                null,
                                accessInfo.getCompressType(),
                                accessInfo.getQuery());
                logGroups.addAll(response.getLogGroups());
                fetchCnt++;
            }
        } catch (LogException e) {
            LOG.warn(String.format("Failed to initialize table schema for shard %s", shard), e);
            return builder.build();
        } catch (ShardNotExistException e) {
            LOG.warn("Shard {} does not exists when initializing schema, skip it.", shard);
            return builder.build();
        }
        if (logGroups.size() < maxFetchLogGroups) {
            LOG.warn(
                    "Fetched {} log groups in {}, less than the configured max fetch log groups {}",
                    logGroups.size(),
                    shard,
                    maxFetchLogGroups);
        }
        for (LogGroupData logGroupData : logGroups) {
            for (FastLog log : logGroupData.GetFastLogGroup().getLogs()) {
                for (FastLogContent content : log.getContents()) {
                    columns.add(content.getKey());
                }
            }
        }
        if (columns.isEmpty()) {
            LOG.warn("There is no column parsed for shard {}", shard);
        }
        columns.forEach(col -> builder.physicalColumn(col, DataTypes.STRING()));
        return builder.build();
    }

    @VisibleForTesting
    CursorInitializer.ShardCursorRetriever getCursorRetriever() {
        return cursorRetriever;
    }
}
