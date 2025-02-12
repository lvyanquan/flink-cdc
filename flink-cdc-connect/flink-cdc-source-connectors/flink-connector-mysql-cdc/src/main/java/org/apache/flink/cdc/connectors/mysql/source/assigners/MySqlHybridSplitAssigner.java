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
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.events.TriggerSplitRequestEvent;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceEnumeratorMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link MySqlSplitAssigner} that splits tables into small chunk splits based on primary key
 * range and chunk size and also continue with a binlog split.
 */
public class MySqlHybridSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlHybridSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final int splitMetaGroupSize;
    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;
    // Whether stop returning splits when invoking getNext method
    private boolean blockSplitAssignmentToSetSignal = false;
    private boolean isInitialBinlogSplitAssignment = true;
    private boolean skipSetIsInsertOnly;

    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    private final SplitEnumeratorContext<MySqlSplit> enumeratorContext;
    private final boolean isCdcYamlSource;
    private MySqlSourceEnumeratorMetrics enumeratorMetrics;

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext,
            boolean isCdcYamlSource) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        enumeratorContext),
                false,
                sourceConfig.getSplitMetaGroupSize(),
                enumeratorContext,
                isCdcYamlSource);
    }

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext,
            boolean isCdcYamlSource) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        enumeratorContext),
                checkpoint.isBinlogSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize(),
                enumeratorContext,
                isCdcYamlSource);
    }

    private MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            boolean isBinlogSplitAssigned,
            int splitMetaGroupSize,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext,
            boolean isCdcYamlSource) {
        this.sourceConfig = sourceConfig;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.enumeratorContext = enumeratorContext;
        this.isCdcYamlSource = isCdcYamlSource;

        try {
            Class enumertorContextClass = enumeratorContext.getClass();
            enumertorContextClass.getMethod("setIsInsertOnly", boolean.class, Runnable.class);
            this.skipSetIsInsertOnly = false;
            LOG.info("Support setIsInsertOnly in enumeratorContext.");
        } catch (NoSuchMethodException e) {
            this.skipSetIsInsertOnly = true;
            LOG.warn(
                    "Cannot find setIsInsertOnly method in enumeratorContext, skip the related code.",
                    e);
        }
    }

    @Override
    public void open() {
        this.enumeratorMetrics =
                new MySqlSourceEnumeratorMetrics(enumeratorContext.metricGroup(), isCdcYamlSource);

        if (isBinlogSplitAssigned) {
            enumeratorMetrics.enterBinlogReading();
        } else {
            enumeratorMetrics.exitBinlogReading();
        }

        snapshotSplitAssigner.setEnumeratorMetrics(enumeratorMetrics);
        snapshotSplitAssigner.open();

        if (snapshotSplitAssigner.shouldEnterProcessingBacklog()) {
            enumeratorContext.setIsProcessingBacklog(true);
            if (!skipSetIsInsertOnly) {
                safelySetInsertOnlySignal(true);
            }
        } else {
            enumeratorContext.setIsProcessingBacklog(false);
        }
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }
        if (!skipSetIsInsertOnly && blockSplitAssignmentToSetSignal) {
            // do not assign split until the insert only signal is sent
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            enumeratorMetrics.exitSnapshotPhase();
            // binlog split assigning
            if (isBinlogSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (AssignerStatus.isInitialAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // Set isInsertOnly to false and return an empty result when the assigner should
                // return a BinlogSplit at the first time.
                // After setting isInsertOnly to false in all readers, the callback will trigger all
                // readers to request splits again.
                // Then the assigner will assign the BinlogSplit.
                if (!skipSetIsInsertOnly && isInitialBinlogSplitAssignment) {
                    safelySetInsertOnlySignal(false);
                    this.isInitialBinlogSplitAssignment = false;
                    return Optional.empty();
                }

                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isBinlogSplitAssigned = true;
                enumeratorMetrics.enterBinlogReading();
                return Optional.of(createBinlogSplit());
            } else if (AssignerStatus.isNewlyAddedAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create binlog, but send event to wake up the binlog reader
                isBinlogSplitAssigned = true;
                enumeratorMetrics.enterBinlogReading();
                return Optional.empty();
            } else {
                // binlog split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public boolean isStreamSplitAssigned() {
        return isBinlogSplitAssigned;
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create binlog split later
                isBinlogSplitAssigned = false;
                enumeratorMetrics.exitBinlogReading();
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits() && isBinlogSplitAssigned;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        snapshotSplitAssigner.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    public boolean noMoreSnapshotSplits() {
        return snapshotSplitAssigner.noMoreSplits();
    }

    // --------------------------------------------------------------------------------------------

    private MySqlBinlogSplit createBinlogSplit() {
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());

        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        BinlogOffset minBinlogOffset = null;
        BinlogOffset maxBinlogOffset = null;
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // find the min and max binlog offset
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            if (maxBinlogOffset == null || binlogOffset.isAfter(maxBinlogOffset)) {
                maxBinlogOffset = binlogOffset;
            }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }

        // If the source is running in snapshot mode, we use the highest watermark among
        // snapshot splits as the ending offset to provide a consistent snapshot view at the moment
        // of high watermark.
        BinlogOffset stoppingOffset = BinlogOffset.ofNonStopping();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxBinlogOffset;
        }

        // the finishedSnapshotSplitInfos is too large for transmission, divide it to groups and
        // then transfer them
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? BinlogOffset.ofEarliest() : minBinlogOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }

    private void safelySetInsertOnlySignal(Boolean isInsertOnly) {
        LOG.info(
                "Current split assignment has been blocked to wait source operator set insertOnly = {} signal.",
                isInsertOnly);
        this.blockSplitAssignmentToSetSignal = true;
        enumeratorContext.setIsInsertOnly(
                isInsertOnly,
                () -> {
                    this.blockSplitAssignmentToSetSignal = false;
                    LOG.info(
                            "The MySqlHybridSplitAssigner triggers all readers to request splits after set insertOnly = {} signal success.",
                            isInsertOnly);
                    for (int subtaskId : getRegisteredReader()) {
                        enumeratorContext.sendEventToSourceReader(
                                subtaskId, new TriggerSplitRequestEvent());
                    }
                });
    }

    private int[] getRegisteredReader() {
        return this.enumeratorContext.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }
}
