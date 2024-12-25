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

package org.apache.flink.cdc.connectors.kafka.json.canal;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaAndConverter;
import org.apache.flink.cdc.connectors.kafka.source.SourceRecordToRecordDataConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link CanalJsonDeserializationSchema} implementation using CONTINUOUS schema inference
 * strategy.
 */
public class CanalJsonContinuousDeserializationSchema extends CanalJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaAndConverter> tableSchemaConverters;
    private final Set<TableId> alreadySendCreateTableTables;

    public CanalJsonContinuousDeserializationSchema(
            @Nullable String database,
            @Nullable String table,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        super(database, table, ignoreParseErrors, primitiveAsString, timestampFormat, zoneId);
        this.reuseEventsToCollect = new ArrayList<>();
        this.tableSchemaConverters = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    protected List<Event> deserialize(
            TableId tableId,
            String type,
            List<String> dataList,
            List<String> oldList,
            List<String> pkNames,
            JsonNode oldFieldJsonNode)
            throws Exception {
        reuseEventsToCollect.clear();

        // Value of some fields could be null before or after, so we should parse all rows' schema
        // and merge them
        List<SourceRecord> dataSourceRecords = new ArrayList<>();
        List<SourceRecord> oldSourceRecords = new ArrayList<>();
        List<SourceRecord> allSourceRecords = new ArrayList<>();
        for (String json : dataList) {
            SourceRecord sourceRecord = getSourceRecordByJson(json);
            dataSourceRecords.add(sourceRecord);
            allSourceRecords.add(sourceRecord);
        }
        for (String json : oldList) {
            SourceRecord sourceRecord = getSourceRecordByJson(json);
            oldSourceRecords.add(sourceRecord);
            allSourceRecords.add(sourceRecord);
        }
        SchemaSpec schemaSpec =
                SchemaUtils.mergeSchemaSpec(
                        allSourceRecords.stream()
                                .map(SourceRecord::getSchema)
                                .collect(Collectors.toList()));

        Schema schema = SchemaUtils.convert(schemaSpec, pkNames);
        List<SchemaChangeEvent> schemaChanges = Collections.emptyList();
        boolean tableSchemaExists = tableSchemaConverters.containsKey(tableId);
        if (tableSchemaExists) {
            Schema currentSchema = tableSchemaConverters.get(tableId).getSchema();
            if (SchemaMergingUtils.isSchemaCompatible(currentSchema, schema)) {
                schema = currentSchema;
            } else {
                schema = SchemaMergingUtils.getLeastCommonSchema(currentSchema, schema);
                schemaChanges =
                        SchemaMergingUtils.getSchemaDifference(tableId, currentSchema, schema);
            }
        }

        boolean needSendCreateTableEvent;
        if (!alreadySendCreateTableTables.contains(tableId)) {
            needSendCreateTableEvent = true;
            if (tableSchemaExists) {
                // After restoring with state, send create table event with schema in state
                // first.
                reuseEventsToCollect.add(
                        new CreateTableEvent(
                                tableId, tableSchemaConverters.get(tableId).getSchema()));
            } else {
                reuseEventsToCollect.add(new CreateTableEvent(tableId, schema));
            }
        } else {
            needSendCreateTableEvent = false;
        }

        TableSchemaAndConverter tableSchemaConverter;
        boolean needUpdateTableSchema = false;
        if (!tableSchemaExists || !schemaChanges.isEmpty()) {
            needUpdateTableSchema = true;
            tableSchemaConverter = TableSchemaAndConverter.of(schema, timestampFormat);
        } else {
            tableSchemaConverter = tableSchemaConverters.get(tableId);
        }
        reuseEventsToCollect.addAll(schemaChanges);

        SourceRecordToRecordDataConverter converter = tableSchemaConverter.getConverter();
        switch (type) {
            case OP_INIT:
            case OP_INSERT:
                for (SourceRecord sourceRecord : dataSourceRecords) {
                    reuseEventsToCollect.add(
                            DataChangeEvent.insertEvent(tableId, converter.convert(sourceRecord)));
                }
                break;
            case OP_DELETE:
                for (SourceRecord sourceRecord : dataSourceRecords) {
                    reuseEventsToCollect.add(
                            DataChangeEvent.deleteEvent(tableId, converter.convert(sourceRecord)));
                }
                break;
            case OP_UPDATE:
                for (int i = 0; i < dataSourceRecords.size(); i++) {
                    SourceRecord oldSourceRecord = oldSourceRecords.get(i);
                    SourceRecord dataSourceRecord = dataSourceRecords.get(i);
                    // fields in "old" (before) means the fields are changed
                    // fields not in "old" (before) means the fields are not changed
                    // so we just copy the not changed fields into before
                    oldSourceRecord =
                            getCompleteBeforeSourceRecord(
                                    schemaSpec,
                                    oldSourceRecord,
                                    dataSourceRecord,
                                    oldFieldJsonNode);
                    RecordData before = converter.convert(oldSourceRecord);
                    RecordData after = converter.convert(dataSourceRecords.get(i));
                    reuseEventsToCollect.add(DataChangeEvent.updateEvent(tableId, before, after));
                }
                break;
            default:
                // data with DDL type has been filtered in parent class
                throw new IOException(String.format("Unknown \"type\" value \"%s\".", type));
        }

        // update schema and states only after parsing successfully
        if (needSendCreateTableEvent) {
            alreadySendCreateTableTables.add(tableId);
        }
        if (needUpdateTableSchema) {
            tableSchemaConverters.put(tableId, tableSchemaConverter);
        }
        return reuseEventsToCollect;
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (!tableSchemaConverters.containsKey(tableId)) {
            return null;
        }
        return tableSchemaConverters.get(tableId).getSchema();
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        if (alreadySendCreateTableTables.contains(tableId)) {
            // If CreateTableEvent has been sent, we should infer SchemaChangeEvent based on the
            // existent schema.
            return;
        }
        if (tableSchemaConverters.containsKey(tableId)) {
            // Merge schema
            Schema currentSchema = tableSchemaConverters.get(tableId).getSchema();
            schema = SchemaMergingUtils.getLeastCommonSchema(currentSchema, schema);
        }
        tableSchemaConverters.put(tableId, TableSchemaAndConverter.of(schema, timestampFormat));
    }

    @VisibleForTesting
    Map<TableId, TableSchemaAndConverter> getTableSchemaConverters() {
        return tableSchemaConverters;
    }

    @VisibleForTesting
    Set<TableId> getAlreadySendCreateTableTables() {
        return alreadySendCreateTableTables;
    }
}
