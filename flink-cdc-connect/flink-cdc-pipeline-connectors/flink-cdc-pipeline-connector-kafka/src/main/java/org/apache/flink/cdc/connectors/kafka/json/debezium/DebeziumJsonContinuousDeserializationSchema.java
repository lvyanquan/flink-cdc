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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

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

import org.jetbrains.annotations.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A {@link DebeziumJsonDeserializationSchema} using CONTINUOUS schema inference strategy. */
public class DebeziumJsonContinuousDeserializationSchema extends DebeziumJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaAndConverter> tableSchemaConverters;
    private final Set<TableId> alreadySendCreateTableTables;

    public DebeziumJsonContinuousDeserializationSchema(
            boolean schemaInclude,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        super(schemaInclude, ignoreParseErrors, primitiveAsString, timestampFormat, zoneId);
        this.reuseEventsToCollect = new ArrayList<>();
        this.tableSchemaConverters = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    protected List<Event> deserialize(
            TableId tableId, String op, @Nullable String before, @Nullable String after)
            throws Exception {
        reuseEventsToCollect.clear();

        String payloadData;
        if (OP_DELETE.equals(op)) {
            if (before == null) {
                throw new IllegalStateException(
                        String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
            }
            payloadData = before;
        } else {
            payloadData = after;
        }

        SourceRecord sourceRecord = getSourceRecordByJson(payloadData);
        SchemaSpec schemaSpec = sourceRecord.getSchema();
        Schema schema = SchemaUtils.convert(schemaSpec);
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
        boolean needUpdateTableSchema;
        if (!tableSchemaExists || !schemaChanges.isEmpty()) {
            needUpdateTableSchema = true;
            tableSchemaConverter = TableSchemaAndConverter.of(schema, timestampFormat);
        } else {
            needUpdateTableSchema = false;
            tableSchemaConverter = tableSchemaConverters.get(tableId);
        }
        reuseEventsToCollect.addAll(schemaChanges);

        SourceRecordToRecordDataConverter converter = tableSchemaConverter.getConverter();
        switch (op) {
            case OP_CREATE:
            case OP_READ:
                reuseEventsToCollect.add(
                        DataChangeEvent.insertEvent(tableId, converter.convert(sourceRecord)));
                break;
            case OP_DELETE:
                reuseEventsToCollect.add(
                        DataChangeEvent.deleteEvent(tableId, converter.convert(sourceRecord)));
                break;
            case OP_UPDATE:
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                SourceRecord beforeSourceRecord = getSourceRecordByJson(before);
                RecordData beforeRecordData = converter.convert(beforeSourceRecord);
                RecordData afterRecordData = converter.convert(sourceRecord);
                reuseEventsToCollect.add(
                        DataChangeEvent.updateEvent(tableId, beforeRecordData, afterRecordData));
                break;
            default:
                throw new IllegalStateException(String.format("Unknown \"op\" value \"%s\".", op));
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

    @VisibleForTesting
    public boolean isSchemaInclude() {
        return schemaInclude;
    }

    @VisibleForTesting
    public boolean isIgnoreParseErrors() {
        return ignoreParseErrors;
    }
}
