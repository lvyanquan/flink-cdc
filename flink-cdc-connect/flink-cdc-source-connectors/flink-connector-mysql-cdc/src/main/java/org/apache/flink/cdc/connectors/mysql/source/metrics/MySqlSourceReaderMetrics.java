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

import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getFetchTimestamp;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getMessageTimestamp;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReaderMetrics.class);

    // Constants
    public static final long UNDEFINED = -1;
    public static final String DATABASE_GROUP_KEY = "database";
    public static final String TABLE_GROUP_KEY = "table";

    // Metric names
    public static final String NUM_SNAPSHOT_RECORDS = "numSnapshotRecords";
    public static final String NUM_INSERT_DML_RECORDS = "numInsertDMLRecords";
    public static final String NUM_UPDATE_DML_RECORDS = "numUpdateDMLRecords";
    public static final String NUM_DELETE_DML_RECORDS = "numDeleteDMLRecords";
    public static final String NUM_DDL_RECORDS = "numDDLRecords";
    public static final String CURRENT_READ_TIMESTAMP_MS = "currentReadTimestampMs";

    // Reader-level metric group
    private final SourceReaderMetricGroup metricGroup;

    // Reader-level metrics
    private final Counter snapshotCounter;
    private final Counter insertCounter;
    private final Counter updateCounter;
    private final Counter deleteCounter;
    private final Counter schemaChangeCounter;

    // Map for managing per-table metrics by table identifier
    // Key: Identifier of the table
    // Value: TableMetrics related to the table
    private final Map<TableId, TableMetrics> tableMetricsMap = new HashMap<>();

    // currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
    // record fetched into the source operator
    private volatile long fetchDelay = 0L;

    // Timestamp when the current processing event was produced in MySQL.
    private volatile long currentReadTimestampMs = UNDEFINED;

    public MySqlSourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        metricGroup.gauge(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> fetchDelay);
        metricGroup.gauge(CURRENT_READ_TIMESTAMP_MS, () -> currentReadTimestampMs);
        snapshotCounter = metricGroup.counter(NUM_SNAPSHOT_RECORDS);
        insertCounter = metricGroup.counter(NUM_INSERT_DML_RECORDS);
        updateCounter = metricGroup.counter(NUM_UPDATE_DML_RECORDS);
        deleteCounter = metricGroup.counter(NUM_DELETE_DML_RECORDS);
        schemaChangeCounter = metricGroup.counter(NUM_DDL_RECORDS);
    }

    /**
     * Mark a source record and process its related metrics.
     *
     * @param record current processing {@link SourceRecord}
     */
    public void markRecord(SourceRecord record) {
        // Update reader-level numRecordsIn
        metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
        // Increase reader and table level input counters
        updateRecordCounters(record);
        // Update currentFetchEventTimeLag
        updateTemporalMetrics(record);
    }

    // ------------------------------- Helper functions -----------------------------

    private TableMetrics getTableMetrics(SourceRecord record) {
        return tableMetricsMap.computeIfAbsent(
                getTableId(record),
                tableId -> new TableMetrics(tableId.catalog(), tableId.table(), metricGroup));
    }

    private void updateRecordCounters(SourceRecord record) {
        TableMetrics tableMetrics = getTableMetrics(record);
        if (isDataChangeRecord(record)) {
            Envelope.Operation op = Envelope.operationFor(record);
            switch (op) {
                case READ:
                    snapshotCounter.inc();
                    tableMetrics.markSnapshotRecord();
                    break;
                case CREATE:
                    insertCounter.inc();
                    tableMetrics.markInsertRecord();
                    break;
                case DELETE:
                    deleteCounter.inc();
                    tableMetrics.markDeleteRecord();
                    break;
                case UPDATE:
                    updateCounter.inc();
                    tableMetrics.markUpdateRecord();
                    break;
            }
        } else if (RecordUtils.isSchemaChangeEvent(record)) {
            schemaChangeCounter.inc();
            tableMetrics.markSchemaChangeRecord();
        }
    }

    private void updateTemporalMetrics(SourceRecord record) {
        Long messageTimestamp = getMessageTimestamp(record);
        if (messageTimestamp != null && messageTimestamp > 0L) {
            // currentReadTimestampMs
            currentReadTimestampMs = messageTimestamp;
            // currentEmitEventTimeLag
            Long fetchTimestamp = getFetchTimestamp(record);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                fetchDelay = fetchTimestamp - messageTimestamp;
            }
        }
    }

    // ----------------------------------- Helper classes --------------------------------

    /**
     * Collection class for managing metrics of a table.
     *
     * <p>Metrics of table level are registered in its corresponding subgroup under the {@link
     * SourceReaderMetricGroup}.
     */
    private static class TableMetrics {

        // Snapshot + binlog
        private final Counter recordsCounter;

        // Snapshot phase
        private final Counter snapshotCounter;

        // Binlog phase
        private final Counter insertCounter;
        private final Counter updateCounter;
        private final Counter deleteCounter;
        private final Counter schemaChangeCounter;

        public TableMetrics(String databaseName, String tableName, MetricGroup parentGroup) {
            MetricGroup metricGroup =
                    parentGroup
                            .addGroup(DATABASE_GROUP_KEY, databaseName)
                            .addGroup(TABLE_GROUP_KEY, tableName);
            recordsCounter = metricGroup.counter(MetricNames.IO_NUM_RECORDS_IN);
            snapshotCounter = metricGroup.counter(NUM_SNAPSHOT_RECORDS);
            insertCounter = metricGroup.counter(NUM_INSERT_DML_RECORDS);
            updateCounter = metricGroup.counter(NUM_UPDATE_DML_RECORDS);
            deleteCounter = metricGroup.counter(NUM_DELETE_DML_RECORDS);
            schemaChangeCounter = metricGroup.counter(NUM_DDL_RECORDS);
        }

        public void markSnapshotRecord() {
            recordsCounter.inc();
            snapshotCounter.inc();
        }

        public void markInsertRecord() {
            recordsCounter.inc();
            insertCounter.inc();
        }

        public void markDeleteRecord() {
            recordsCounter.inc();
            deleteCounter.inc();
        }

        public void markUpdateRecord() {
            recordsCounter.inc();
            updateCounter.inc();
        }

        public void markSchemaChangeRecord() {
            recordsCounter.inc();
            schemaChangeCounter.inc();
        }
    }
}
