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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.SchemaAwareDeserializationSchema;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnSpec;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Deserialization schema that deserializes a Canal JSON bytes into FlinkCDC pipeline internal data
 * structure {@link Event}. It also maintains table schemas for schema inference and schema
 * evolution.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public abstract class CanalJsonDeserializationSchema
        implements SchemaAwareDeserializationSchema<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(CanalJsonDeserializationSchema.class);
    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");

    private static final String DATABASE_KEY = "database";
    private static final String TABLE_KEY = "table";

    protected static final String FIELD_OLD = "old";
    // For compatibility with DTS on Alibaba Cloud
    protected static final String OP_INIT = "INIT";
    protected static final String OP_INSERT = "INSERT";
    protected static final String OP_UPDATE = "UPDATE";
    protected static final String OP_DELETE = "DELETE";
    protected static final String OP_CREATE = "CREATE";
    protected static final String OP_ALTER = "ALTER";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Pattern databasePattern;
    private final Pattern tablePattern;
    private final boolean ignoreParseErrors;
    protected final TimestampFormat timestampFormat;
    protected final JsonRowDataDeserializationSchema jsonDeserializer;
    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;

    public CanalJsonDeserializationSchema(
            @Nullable String database,
            @Nullable String table,
            boolean ignoreParseErrors,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        this.databasePattern = database == null ? null : Pattern.compile(database);
        this.tablePattern = table == null ? null : Pattern.compile(table);
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;

        final RowType jsonRowType = createJsonRowType();
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
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
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonDeserializer.open(context);
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<Event>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<Event> out) throws IOException {
        if (message == null || message.length == 0) {
            return;
        }
        List<Event> events;
        // A big try catch to protect the processing.
        try {
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            JsonNode databaseNode = root.get(DATABASE_KEY);
            JsonNode tableNode = root.get(TABLE_KEY);
            if (databaseNode == null || tableNode == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot get database or table name in Canal JSON '%s'.",
                                new String(message)));
            }
            String database = databaseNode.asText();
            String table = tableNode.asText();
            if (databasePattern != null && !databasePattern.matcher(database).matches()) {
                return;
            }
            if (tablePattern != null && !tablePattern.matcher(table).matches()) {
                return;
            }
            TableId tableId = TableId.tableId(database, table);

            final GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            String type = row.getString(2).toString();
            if (OP_CREATE.equals(type) || OP_ALTER.equals(type)) {
                // "type" is "CREATE" or "ALTER" which means this is a DDL change event, and we
                // should skip it.
                return;
            }

            List<String> dataList = getStringArray(row, 0);
            List<String> oldList = getStringArray(row, 1);
            List<String> pkNames = getStringArray(row, 3);

            events = deserialize(tableId, type, dataList, oldList, pkNames, root.get(FIELD_OLD));
        } catch (Throwable t) {
            String errorMessage =
                    String.format("Corrupt Canal JSON message '%s'.", new String(message));
            if (ignoreParseErrors) {
                LOG.warn(errorMessage, t);
                return;
            } else {
                throw new IOException(errorMessage, t);
            }
        }
        // Send all events after try catch.
        events.forEach(out::collect);
    }

    /**
     * Return events by Canal JSON data fields. Throw exception directly in this method if parse
     * error. {@link CanalJsonDeserializationSchema#deserialize(byte[] message, Collector out)} will
     * handle exception according to ignoreParseError config.
     */
    protected abstract List<Event> deserialize(
            TableId tableId,
            String type,
            List<String> dataList,
            List<String> oldList,
            List<String> pkNames,
            JsonNode oldFiledJsonNode)
            throws Exception;

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    protected SourceRecord getSourceRecordByJson(String json) throws IOException {
        JsonParser root = objectMapper.getFactory().createParser(json);
        if (root.currentToken() == null) {
            root.nextToken();
        }
        return jsonToSourceRecordConverter.convert(root);
    }

    protected SourceRecord getCompleteBeforeSourceRecord(
            SchemaSpec mergedSchema, SourceRecord old, SourceRecord data, JsonNode oldField) {
        Map<String, Object> oldValues = getFieldValues(old);
        Map<String, Object> newValues = getFieldValues(data);
        SchemaSpec.Builder builder = SchemaSpec.newBuilder();

        GenericRowData rowData = new GenericRowData(mergedSchema.getColumnCount());
        List<ColumnSpec> columns = mergedSchema.getColumns();

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            if (oldValues.containsKey(columnName)) {
                // old value is not null
                rowData.setField(i, oldValues.get(columnName));
                builder.column(old.getSchema().getColumn(columnName).get());
            } else if (oldField.findValue(columnName) != null) {
                // old value is null
                builder.column(columns.get(i));
            } else {
                // old value is same as new value
                rowData.setField(i, newValues.get(columnName));
                builder.column(data.getSchema().getColumn(columnName).orElse(columns.get(i)));
            }
        }
        return new SourceRecord(OBJECT_PATH_PLACEHOLDER, builder.build(), rowData);
    }

    private Map<String, Object> getFieldValues(SourceRecord sourceRecord) {
        Map<String, Object> fieldValues = new HashMap<>();
        List<String> oldFields = sourceRecord.getSchema().getColumnNames();
        for (int i = 0; i < oldFields.size(); i++) {
            fieldValues.put(oldFields.get(i), ((GenericRowData) sourceRecord.getRow()).getField(i));
        }
        return fieldValues;
    }

    private static List<String> getStringArray(GenericRowData row, int pos) {
        ArrayData arrayData = row.getArray(pos);
        if (arrayData == null) {
            return Collections.emptyList();
        }
        List<String> stringList = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); i++) {
            stringList.add(arrayData.getString(i).toString());
        }
        return stringList;
    }

    private static RowType createJsonRowType() {
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("old", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("type", DataTypes.STRING()),
                        DataTypes.FIELD("pkNames", DataTypes.ARRAY(DataTypes.STRING())));
        return (RowType) root.getLogicalType();
    }
}
