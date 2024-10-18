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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaAndConverter;
import org.apache.flink.cdc.connectors.kafka.source.SourceRecordToRecordDataConverter;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaEvolveManager;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaEvolveResult;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
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
 * Deserialization schema that deserializes a Debezium JSON bytes into FlinkCDC pipeline internal
 * data structure {@link Event}. It also maintains table schemas by schema inference and schema
 * evolution.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class DebeziumJsonDeserializationSchema implements SchemaAwareDeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(DebeziumJsonDeserializationSchema.class);

    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

    protected static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of %s message is null, "
                    + "if you are using Debezium Postgres Connector, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final boolean schemaInclude;
    private final boolean ignoreParseErrors;
    private final TimestampFormat timestampFormat;

    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;

    private final Map<TableId, TableSchemaAndConverter> tableSchemaConverters;
    private final Set<TableId> alreadySendCreateTableTables;

    public DebeziumJsonDeserializationSchema(
            boolean schemaInclude,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        this.schemaInclude = schemaInclude;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;

        final RowType jsonRowType = createJsonRowType(schemaInclude);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used
                        TypeInformation.of(RowData.class),
                        false,
                        false,
                        timestampFormat,
                        zoneId);
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        this.jsonToSourceRecordConverter =
                new JsonToSourceRecordConverter(
                        // the object path is never used
                        OBJECT_PATH_PLACEHOLDER,
                        new RowType(Collections.emptyList()),
                        new JsonToRowDataConverters(false, false, timestampFormat, zoneId),
                        false,
                        false,
                        primitiveAsString);
        this.tableSchemaConverters = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<Event>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<Event> out) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        TableId tableId;
        TableSchemaAndConverter tableSchemaConverter;
        boolean needUpdateTableSchema = false;
        List<Event> eventsToCollect = new ArrayList<>();
        // a big try catch to protect the processing.
        // collect events and update schema state should after try catch.
        try {
            GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            GenericRowData payload = schemaInclude ? (GenericRowData) row.getField(0) : row;

            String database = readProperty(payload, 3, "db");
            String table = readProperty(payload, 3, "table");
            if (database == null || table == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot get database or table name in Debezium JSON '%s'.",
                                new String(message)));
            }

            tableId = TableId.tableId(database, table);

            StringData before = payload.getString(0);
            StringData after = payload.getString(1);
            String op = payload.getField(2).toString();

            String payloadData;
            if (OP_DELETE.equals(op)) {
                if (before == null) {
                    throw new IllegalStateException(
                            String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
                }
                payloadData = before.toString();
            } else {
                payloadData = after.toString();
            }

            SourceRecord sourceRecord = getSourceRecordByJson(payloadData);
            SchemaSpec schemaSpec = sourceRecord.getSchema();
            Schema schema = SchemaUtils.convert(schemaSpec);
            List<SchemaChangeEvent> schemaChanges = Collections.emptyList();
            boolean tableSchemaExists = tableSchemaConverters.containsKey(tableId);
            if (tableSchemaExists) {
                SchemaEvolveResult schemaEvolveResult =
                        SchemaEvolveManager.evolveSchema(
                                tableId, tableSchemaConverters.get(tableId).getSchema(), schema);
                if (schemaEvolveResult.isIncompatible()) {
                    throw new IllegalStateException(schemaEvolveResult.getIncompatibleReason());
                }
                if (schemaEvolveResult.isCompatibleAfterEvolution()) {
                    schema = schemaEvolveResult.getSchemaAfterEvolve();
                    schemaChanges = schemaEvolveResult.getSchemaChanges();
                }
            }

            if (!alreadySendCreateTableTables.contains(tableId)) {
                if (tableSchemaExists) {
                    // After restoring with state, send create table event with schema in state
                    // first.
                    eventsToCollect.add(
                            new CreateTableEvent(
                                    tableId, tableSchemaConverters.get(tableId).getSchema()));
                } else {
                    eventsToCollect.add(new CreateTableEvent(tableId, schema));
                }
            }

            if (!tableSchemaExists || !schemaChanges.isEmpty()) {
                needUpdateTableSchema = true;
                tableSchemaConverter = TableSchemaAndConverter.of(schema, timestampFormat);
            } else {
                tableSchemaConverter = tableSchemaConverters.get(tableId);
            }
            eventsToCollect.addAll(schemaChanges);

            SourceRecordToRecordDataConverter converter = tableSchemaConverter.getConverter();
            switch (op) {
                case OP_CREATE:
                case OP_READ:
                    eventsToCollect.add(
                            DataChangeEvent.insertEvent(tableId, converter.convert(sourceRecord)));
                    break;
                case OP_DELETE:
                    eventsToCollect.add(
                            DataChangeEvent.deleteEvent(tableId, converter.convert(sourceRecord)));
                    break;
                case OP_UPDATE:
                    if (before == null) {
                        throw new IllegalStateException(
                                String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
                    }
                    SourceRecord beforeSourceRecord = getSourceRecordByJson(before.toString());
                    RecordData beforeRecordData = converter.convert(beforeSourceRecord);
                    RecordData afterRecordData = converter.convert(sourceRecord);
                    eventsToCollect.add(
                            DataChangeEvent.updateEvent(
                                    tableId, beforeRecordData, afterRecordData));
                    break;
                default:
                    throw new IOException(String.format("Unknown \"op\" value \"%s\".", op));
            }
        } catch (Throwable t) {
            String errorMessage =
                    String.format("Corrupt Debezium JSON message '%s'.", new String(message));
            if (ignoreParseErrors) {
                LOG.warn(errorMessage, t);
                return;
            } else {
                throw new IOException(errorMessage, t);
            }
        }
        // collect events and update states only after parsing successfully
        eventsToCollect.forEach(
                event -> {
                    out.collect(event);
                    if (event instanceof CreateTableEvent) {
                        alreadySendCreateTableTables.add(tableId);
                    }
                });
        if (needUpdateTableSchema) {
            tableSchemaConverters.put(tableId, tableSchemaConverter);
        }
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
        tableSchemaConverters.put(tableId, TableSchemaAndConverter.of(schema, timestampFormat));
    }

    private SourceRecord getSourceRecordByJson(String json) throws IOException {
        JsonParser root = objectMapper.getFactory().createParser(json);
        if (root.currentToken() == null) {
            root.nextToken();
        }
        return jsonToSourceRecordConverter.convert(root);
    }

    private static RowType createJsonRowType(boolean schemaInclude) {
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("before", DataTypes.STRING()),
                        DataTypes.FIELD("after", DataTypes.STRING()),
                        DataTypes.FIELD("op", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "source",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.STRING().nullable())
                                        .nullable()));
        DataType root = payload;

        if (schemaInclude) {
            root = DataTypes.ROW(DataTypes.FIELD("payload", payload));
        }

        return (RowType) root.getLogicalType();
    }

    private static String readProperty(GenericRowData row, int pos, String key) {
        GenericMapData map = (GenericMapData) row.getMap(pos);
        if (map == null) {
            return null;
        }
        StringData value = (StringData) map.get(StringData.fromString(key));
        return value == null ? null : value.toString();
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
