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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaAndConverter;
import org.apache.flink.cdc.connectors.kafka.source.SourceRecordToRecordDataConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.jetbrains.annotations.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link CanalJsonDeserializationSchema} implementation using STATIC schema inference strategy.
 */
public class CanalJsonStaticDeserializationSchema extends CanalJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaHelper> staticTableSchemaHelpers;
    private final Set<TableId> alreadySendCreateTableTables;

    public CanalJsonStaticDeserializationSchema(
            @Nullable String database,
            @Nullable String table,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        super(database, table, ignoreParseErrors, primitiveAsString, timestampFormat, zoneId);
        this.reuseEventsToCollect = new ArrayList<>();
        this.staticTableSchemaHelpers = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    protected List<Event> deserialize(
            TableId tableId,
            String type,
            List<String> dataList,
            List<String> oldList,
            List<String> pkNames,
            JsonNode oldField)
            throws Exception {
        if (!staticTableSchemaHelpers.containsKey(tableId)) {
            throw new IllegalStateException(
                    String.format("Unknown initial schema for table <%s>.", tableId));
        }

        reuseEventsToCollect.clear();
        TableSchemaHelper schemaHelper = staticTableSchemaHelpers.get(tableId);
        Schema schema = schemaHelper.schema;
        SourceRecordToRecordDataConverter converter =
                schemaHelper.tableSchemaConverter.getConverter();

        // ensure primary keys not changed
        if (!Objects.equals(schemaHelper.pk, new HashSet<>(pkNames))) {
            throw new IllegalStateException(
                    String.format(
                            "Primary keys has changed, initial pk is %s, but current pk is %s",
                            schema.primaryKeys(), pkNames));
        }

        boolean needSendCreateTableEvent;
        if (!alreadySendCreateTableTables.contains(tableId)) {
            needSendCreateTableEvent = true;
            reuseEventsToCollect.add(new CreateTableEvent(tableId, schemaHelper.schema));
        } else {
            needSendCreateTableEvent = false;
        }

        switch (type) {
            case OP_INIT:
            case OP_INSERT:
                for (String json : dataList) {
                    reuseEventsToCollect.add(
                            DataChangeEvent.insertEvent(
                                    tableId, converter.convert(getSourceRecordByJson(json))));
                }
                break;
            case OP_DELETE:
                for (String json : dataList) {
                    reuseEventsToCollect.add(
                            DataChangeEvent.deleteEvent(
                                    tableId, converter.convert(getSourceRecordByJson(json))));
                }
                break;
            case OP_UPDATE:
                for (int i = 0; i < dataList.size(); i++) {
                    SourceRecord old = getSourceRecordByJson(oldList.get(i));
                    SourceRecord data = getSourceRecordByJson(dataList.get(i));
                    old =
                            getCompleteBeforeSourceRecord(
                                    schemaHelper.schemaSpec, old, data, oldField);
                    RecordData before = converter.convert(old);
                    RecordData after = converter.convert(data);
                    reuseEventsToCollect.add(DataChangeEvent.updateEvent(tableId, before, after));
                }
                break;
            default:
                // data with DDL type has been filtered in parent class
                throw new UnsupportedOperationException(
                        String.format("Unknown \"type\" value \"%s\".", type));
        }

        // update states only after parsing successfully
        if (needSendCreateTableEvent) {
            alreadySendCreateTableTables.add(tableId);
        }
        return reuseEventsToCollect;
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (!staticTableSchemaHelpers.containsKey(tableId)) {
            return null;
        }
        return staticTableSchemaHelpers.get(tableId).schema;
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        if (staticTableSchemaHelpers.containsKey(tableId)) {
            // keep the first registered table schema as initial table schema
            return;
        }
        staticTableSchemaHelpers.put(tableId, new TableSchemaHelper(schema, timestampFormat));
    }

    @VisibleForTesting
    Map<TableId, TableSchemaHelper> getStaticTableSchemaHelpers() {
        return staticTableSchemaHelpers;
    }

    @VisibleForTesting
    Set<TableId> getAlreadySendCreateTableTables() {
        return alreadySendCreateTableTables;
    }

    static class TableSchemaHelper {
        Schema schema;
        SchemaSpec schemaSpec;
        Set<String> pk;
        TableSchemaAndConverter tableSchemaConverter;

        TableSchemaHelper(Schema schema, TimestampFormat timestampFormat) {
            this.schema = schema;
            this.schemaSpec = SchemaUtils.convert(schema);
            this.pk = new HashSet<>(schema.primaryKeys());
            this.tableSchemaConverter = TableSchemaAndConverter.of(schema, timestampFormat);
        }
    }
}
