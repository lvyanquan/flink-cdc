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

package org.apache.flink.cdc.connectors.mysql.source.metrics;

import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics.DEFAULT_GROUP_VALUE;
import static org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics.NAMESPACE_GROUP_KEY;
import static org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics.SCHEMA_GROUP_KEY;
import static org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics.TABLE_GROUP_KEY;

/** A collection class for handling metrics in {@link MySqlSourceEnumerator}. */
public class MySqlSourceEnumeratorMetrics {
    // Constants
    public static final int UNDEFINED = 0;
    // Metric names
    public static final String IS_SNAPSHOTTING = "isSnapshotting";
    public static final String IS_BINLOG_READING = "isBinlogReading";
    public static final String NUM_TABLES_SNAPSHOTTED = "numTablesSnapshotted";
    public static final String NUM_TABLES_REMAINING = "numTablesRemaining";
    public static final String NUM_SNAPSHOT_SPLITS_PROCESSED = "numSnapshotSplitsProcessed";
    public static final String NUM_SNAPSHOT_SPLITS_REMAINING = "numSnapshotSplitsRemaining";
    private final SplitEnumeratorMetricGroup metricGroup;
    private volatile int isSnapshotting = UNDEFINED;
    private volatile int isBinlogReading = UNDEFINED;
    private volatile int numTablesRemaining = 0;
    // Map for managing per-table metrics by table identifier
    // Key: Identifier of the table
    // Value: TableMetrics related to the table
    private final Map<TableId, TableMetrics> tableMetricsMap = new HashMap<>();

    public MySqlSourceEnumeratorMetrics(SplitEnumeratorMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        metricGroup.gauge(IS_SNAPSHOTTING, () -> isSnapshotting);
        metricGroup.gauge(IS_BINLOG_READING, () -> isBinlogReading);
        metricGroup.gauge(NUM_TABLES_REMAINING, () -> numTablesRemaining);
    }

    public void enterSnapshotPhase() {
        this.isSnapshotting = 1;
    }

    public void exitSnapshotPhase() {
        this.isSnapshotting = 0;
    }

    public void enterBinlogReading() {
        this.isBinlogReading = 1;
    }

    public void exitBinlogReading() {
        this.isBinlogReading = 0;
    }

    public void registerMetrics(
            Gauge<Integer> numTablesSnapshotted,
            Gauge<Integer> numSnapshotSplitsProcessed,
            Gauge<Integer> numSnapshotSplitsRemaining) {
        metricGroup.gauge(NUM_TABLES_SNAPSHOTTED, numTablesSnapshotted);
        metricGroup.gauge(NUM_SNAPSHOT_SPLITS_PROCESSED, numSnapshotSplitsProcessed);
        metricGroup.gauge(NUM_SNAPSHOT_SPLITS_REMAINING, numSnapshotSplitsRemaining);
    }

    public void addNewTables(int numNewTables) {
        numTablesRemaining += numNewTables;
    }

    public void startSnapshotTables(int numSnapshottedTables) {
        numTablesRemaining -= numSnapshottedTables;
    }

    public TableMetrics getTableMetrics(TableId tableId) {
        return tableMetricsMap.computeIfAbsent(
                tableId, key -> new TableMetrics(key.catalog(), key.table(), metricGroup));
    }

    // ----------------------------------- Helper classes --------------------------------

    /**
     * Collection class for managing metrics of a table.
     *
     * <p>Metrics of table level are registered in its corresponding subgroup under the {@link
     * SplitEnumeratorMetricGroup}.
     */
    public static class TableMetrics {
        private AtomicInteger numSnapshotSplitsProcessed = new AtomicInteger(0);
        private AtomicInteger numSnapshotSplitsRemaining = new AtomicInteger(0);

        public TableMetrics(String databaseName, String tableName, MetricGroup parentGroup) {
            MetricGroup metricGroup =
                    parentGroup
                            .addGroup(NAMESPACE_GROUP_KEY, DEFAULT_GROUP_VALUE)
                            .addGroup(SCHEMA_GROUP_KEY, databaseName)
                            .addGroup(TABLE_GROUP_KEY, tableName);
            metricGroup.gauge(
                    NUM_SNAPSHOT_SPLITS_PROCESSED, () -> numSnapshotSplitsProcessed.intValue());
            metricGroup.gauge(
                    NUM_SNAPSHOT_SPLITS_REMAINING, () -> numSnapshotSplitsRemaining.intValue());
        }

        public void addNewSplits(int numNewSplits) {
            numSnapshotSplitsRemaining.getAndAdd(numNewSplits);
        }

        public void addProcessedSplits(int numProcessedSplits) {
            numSnapshotSplitsProcessed.getAndAdd(numProcessedSplits);
        }

        public void reprocessSplits(int numSplitsReprocess) {
            addNewSplits(numSplitsReprocess);
            numSnapshotSplitsProcessed.getAndUpdate(num -> num - numSplitsReprocess);
        }

        public void finishProcessSplits(int numSplitsProcessed) {
            addProcessedSplits(numSplitsProcessed);
            numSnapshotSplitsRemaining.getAndUpdate(num -> num - numSplitsProcessed);
        }
    }
}
