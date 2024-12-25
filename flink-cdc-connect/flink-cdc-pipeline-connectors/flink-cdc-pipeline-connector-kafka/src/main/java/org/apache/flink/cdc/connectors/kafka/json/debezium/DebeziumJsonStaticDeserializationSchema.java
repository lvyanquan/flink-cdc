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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaAndConverter;
import org.apache.flink.cdc.connectors.kafka.source.SourceRecordToRecordDataConverter;
import org.apache.flink.formats.common.TimestampFormat;

import org.jetbrains.annotations.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DebeziumJsonDeserializationSchema} implementation using STATIC schema inference
 * strategy.
 */
public class DebeziumJsonStaticDeserializationSchema extends DebeziumJsonDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaAndConverter> staticTableSchemaConverters;
    private final Set<TableId> alreadySendCreateTableTables;

    public DebeziumJsonStaticDeserializationSchema(
            boolean schemaInclude,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        super(schemaInclude, ignoreParseErrors, primitiveAsString, timestampFormat, zoneId);
        this.reuseEventsToCollect = new ArrayList<>();
        this.staticTableSchemaConverters = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    protected List<Event> deserialize(
            TableId tableId, String op, @Nullable String before, @Nullable String after)
            throws Exception {
        if (!staticTableSchemaConverters.containsKey(tableId)) {
            throw new IllegalStateException(
                    String.format("Unknown initial schema for table <%s>.", tableId));
        }

        reuseEventsToCollect.clear();
        TableSchemaAndConverter tableSchemaConverter = staticTableSchemaConverters.get(tableId);
        SourceRecordToRecordDataConverter converter = tableSchemaConverter.getConverter();

        boolean needSendCreateTableEvent;
        if (!alreadySendCreateTableTables.contains(tableId)) {
            needSendCreateTableEvent = true;
            reuseEventsToCollect.add(
                    new CreateTableEvent(tableId, tableSchemaConverter.getSchema()));
        } else {
            needSendCreateTableEvent = false;
        }

        switch (op) {
            case OP_CREATE:
            case OP_READ:
                reuseEventsToCollect.add(
                        DataChangeEvent.insertEvent(
                                tableId, converter.convert(getSourceRecordByJson(after))));
                break;
            case OP_DELETE:
                reuseEventsToCollect.add(
                        DataChangeEvent.deleteEvent(
                                tableId, converter.convert(getSourceRecordByJson(before))));
                break;
            case OP_UPDATE:
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                }
                reuseEventsToCollect.add(
                        DataChangeEvent.updateEvent(
                                tableId,
                                converter.convert(getSourceRecordByJson(before)),
                                converter.convert(getSourceRecordByJson(after))));
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown \"op\" value \"%s\".", op));
        }

        // update status only after parsing successfully
        if (needSendCreateTableEvent) {
            alreadySendCreateTableTables.add(tableId);
        }
        return reuseEventsToCollect;
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (!staticTableSchemaConverters.containsKey(tableId)) {
            return null;
        }
        return staticTableSchemaConverters.get(tableId).getSchema();
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        if (staticTableSchemaConverters.containsKey(tableId)) {
            // keep the first registered table schema as initial table schema
            return;
        }
        staticTableSchemaConverters.put(
                tableId, TableSchemaAndConverter.of(schema, timestampFormat));
    }

    @VisibleForTesting
    Map<TableId, TableSchemaAndConverter> getStaticTableSchemaConverters() {
        return staticTableSchemaConverters;
    }

    @VisibleForTesting
    Set<TableId> getAlreadySendCreateTableTables() {
        return alreadySendCreateTableTables;
    }
}
