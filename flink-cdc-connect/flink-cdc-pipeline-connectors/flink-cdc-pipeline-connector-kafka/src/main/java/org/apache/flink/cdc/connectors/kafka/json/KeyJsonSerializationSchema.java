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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Serialization schema from FlinkCDC pipeline internal data structure {@link Event} to normal JSON.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class KeyJsonSerializationSchema implements SerializationSchema<Event> {
    private static final long serialVersionUID = 1L;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. */
    private final String mapNullKeyLiteral;

    /** Flag indicating whether to serialize all decimals as plain numbers. */
    private final boolean encodeDecimalAsPlainNumber;

    private final boolean writeNullProperties;
    private final ZoneId zoneId;

    /**
     * A map of {@link TableId} and its {@link SerializationSchema} to serialize Debezium JSON data.
     */
    private final Map<TableId, TableSchemaInfo> jsonSerializers;

    private InitializationContext context;

    public KeyJsonSerializationSchema(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean writeNullProperties,
            ZoneId zoneId) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.writeNullProperties = writeNullProperties;
        this.zoneId = zoneId;
        this.jsonSerializers = new HashMap<>();
    }

    @Override
    public void open(InitializationContext context) {
        this.context = context;
    }

    @Override
    public byte[] serialize(Event event) {
        if (event instanceof SchemaChangeEvent) {
            Schema schema;
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schema = createTableEvent.getSchema();
            } else {
                schema =
                        SchemaUtils.applySchemaChangeEvent(
                                jsonSerializers.get(schemaChangeEvent.tableId()).getSchema(),
                                schemaChangeEvent);
            }

            JsonRowDataSerializationSchema jsonSerializer =
                    new JsonRowDataSerializationSchema(
                            getPrimaryKeysRowType(schema),
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            writeNullProperties);
            try {
                jsonSerializer.open(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            jsonSerializers.put(
                    schemaChangeEvent.tableId(),
                    new TableSchemaInfo(
                            schemaChangeEvent.tableId(), schema, jsonSerializer, zoneId));
            return null;
        }

        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        try {
            TableSchemaInfo tableSchemaInfo = jsonSerializers.get(dataChangeEvent.tableId());
            switch (dataChangeEvent.op()) {
                case INSERT:
                case UPDATE:
                case REPLACE:
                    return tableSchemaInfo
                            .getSerializationSchema()
                            .serialize(
                                    getKeyRowDataFromRecordData(
                                            tableSchemaInfo, dataChangeEvent.after()));
                case DELETE:
                    return tableSchemaInfo
                            .getSerializationSchema()
                            .serialize(
                                    getKeyRowDataFromRecordData(
                                            tableSchemaInfo, dataChangeEvent.before()));
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for OperationType.",
                                    dataChangeEvent.op()));
            }
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize event '%s'.", event), t);
        }
    }

    public RowData getKeyRowDataFromRecordData(
            TableSchemaInfo tableSchemaInfo, RecordData recordData) {
        List<String> primaryKeys = tableSchemaInfo.getSchema().primaryKeys();

        GenericRowData genericRowData = new GenericRowData(primaryKeys.size());
        for (int i = 0; i < primaryKeys.size(); i++) {
            int foundIndex =
                    tableSchemaInfo.getSchema().getColumnNames().indexOf(primaryKeys.get(i));
            genericRowData.setField(
                    i,
                    tableSchemaInfo.getFieldGetters().get(foundIndex).getFieldOrNull(recordData));
        }
        return genericRowData;
    }

    private org.apache.flink.table.types.logical.RowType getPrimaryKeysRowType(Schema schema) {
        List<DataTypes.Field> fields = new ArrayList<>();
        List<String> primaryKeys = schema.primaryKeys();
        for (String primaryKey : primaryKeys) {
            DataType type = schema.getColumn(primaryKey).get().getType();
            fields.add(DataTypes.FIELD(primaryKey, DataTypeUtils.toFlinkDataType(type)));
        }
        return (RowType) DataTypes.ROW(fields).getLogicalType();
    }
}
