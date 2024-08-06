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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.mysql.source.connection.JdbcConnectionPools;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.util.CollectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A {@link MySqlSplitAssigner} which only read binlog from current binlog position. */
public class MySqlBinlogSplitAssigner implements MySqlSplitAssigner {

    public static final String BINLOG_SPLIT_ID = "binlog-split";

    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;

    private final SplitEnumeratorContext<MySqlSplit> enumeratorContext;
    private MySqlSourceEnumeratorMetrics enumeratorMetrics;

    public MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(sourceConfig, false, enumeratorContext);
    }

    public MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig,
            BinlogPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(sourceConfig, checkpoint.isBinlogSplitAssigned(), enumeratorContext);
    }

    private MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig,
            boolean isBinlogSplitAssigned,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this.sourceConfig = sourceConfig;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.enumeratorContext = enumeratorContext;
    }

    @Override
    public void open() {
        this.enumeratorMetrics = new MySqlSourceEnumeratorMetrics(enumeratorContext.metricGroup());
        if (isBinlogSplitAssigned) {
            enumeratorMetrics.enterBinlogReading();
        } else {
            enumeratorMetrics.exitBinlogReading();
        }
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (isBinlogSplitAssigned) {
            return Optional.empty();
        } else {
            isBinlogSplitAssigned = true;
            enumeratorMetrics.enterBinlogReading();
            return Optional.of(createBinlogSplit());
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return false;
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return Collections.emptyList();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        if (!CollectionUtil.isNullOrEmpty(splits)) {
            // we don't store the split, but will re-create binlog split later
            isBinlogSplitAssigned = false;
            enumeratorMetrics.exitBinlogReading();
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new BinlogPendingSplitsState(isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
    }

    @Override
    public boolean noMoreSplits() {
        return isBinlogSplitAssigned;
    }

    @Override
    public void startAssignNewlyAddedTables() {}

    @Override
    public void onBinlogSplitUpdated() {}

    @Override
    public void close() throws IOException {
        // clear jdbc connection pools
        JdbcConnectionPools.getInstance().clear();
    }

    // ------------------------------------------------------------------------------------------

    private MySqlBinlogSplit createBinlogSplit() {
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                sourceConfig.getStartupOptions().binlogOffset,
                BinlogOffset.ofNonStopping(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
    }
}
