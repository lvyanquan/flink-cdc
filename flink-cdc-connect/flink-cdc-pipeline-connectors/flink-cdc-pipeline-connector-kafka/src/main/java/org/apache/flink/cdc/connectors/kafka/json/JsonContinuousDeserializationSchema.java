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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.cdc.connectors.kafka.source.SourceRecordToRecordDataConverter;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataInSourceRecordConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deserialization schema that deserializes a JSON bytes into FlinkCDC pipeline internal data
 * structure {@link Event} using CONTINUOUS schema inference strategy.
 */
public class JsonContinuousDeserializationSchema
        implements SchemaAwareDeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(JsonContinuousDeserializationSchema.class);
    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");

    private final boolean ignoreParseErrors;
    private final TimestampFormat timestampFormat;

    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaAndConverter> tableSchemaConverters;
    private final Set<TableId> alreadySendCreateTableTables;

    public JsonContinuousDeserializationSchema(
            boolean flattenNestedColumn,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;

        this.jsonToSourceRecordConverter =
                new JsonToSourceRecordConverter(
                        OBJECT_PATH_PLACEHOLDER,
                        new RowType(Collections.emptyList()),
                        new JsonToRowDataInSourceRecordConverters(
                                false, false, timestampFormat, zoneId),
                        false,
                        flattenNestedColumn,
                        primitiveAsString);

        this.reuseEventsToCollect = new ArrayList<>();
        this.tableSchemaConverters = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], TableId, Collector<Event>) instead.");
    }

    @Override
    public void deserialize(byte[] message, TableId tableId, Collector<Event> out)
            throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        reuseEventsToCollect.clear();
        try (JsonParser root = objectMapper.getFactory().createParser(message)) {
            if (root.currentToken() == null) {
                root.nextToken();
            }
            SourceRecord sourceRecord = jsonToSourceRecordConverter.convert(root);

            Schema schema = SchemaUtils.convert(sourceRecord.getSchema());
            boolean tableSchemaExists = tableSchemaConverters.containsKey(tableId);

            List<SchemaChangeEvent> schemaChanges = Collections.emptyList();
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
            RecordData recordData = converter.convert(sourceRecord);
            reuseEventsToCollect.add(DataChangeEvent.insertEvent(tableId, recordData));

            // update schema and states only after parsing successfully
            if (needSendCreateTableEvent) {
                alreadySendCreateTableTables.add(tableId);
            }
            if (needUpdateTableSchema) {
                tableSchemaConverters.put(tableId, tableSchemaConverter);
            }
        } catch (Throwable t) {
            String errorMessage =
                    String.format("Failed to deserialize JSON '%s'.", new String(message));
            if (ignoreParseErrors) {
                LOG.warn(errorMessage, t);
                return;
            }
            throw new IOException(errorMessage, t);
        }
        reuseEventsToCollect.forEach(out::collect);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
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
