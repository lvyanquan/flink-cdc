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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Serialization schema from FlinkCDC pipeline internal data structure {@link Event} to normal JSON.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class UpsertKafkaJsonSerializationSchema implements SerializationSchema<Event> {
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
    private final boolean ignoreNullFields;
    private final ZoneId zoneId;

    /**
     * A map of {@link TableId} and its {@link SerializationSchema} to serialize Debezium JSON data.
     */
    private final Map<TableId, TableSchemaInfo> jsonSerializers;

    private InitializationContext context;

    public UpsertKafkaJsonSerializationSchema(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber,
            boolean writeNullProperties,
            ZoneId zoneId,
            boolean ignoreNullFields) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.writeNullProperties = writeNullProperties;
        this.zoneId = zoneId;
        this.jsonSerializers = new HashMap<>();
        this.ignoreNullFields = ignoreNullFields;
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
            LogicalType rowType =
                    DataTypeUtils.toFlinkDataType(schema.toRowDataType()).getLogicalType();
            JsonRowDataSerializationSchema jsonSerializer =
                    new JsonRowDataSerializationSchema(
                            (org.apache.flink.table.types.logical.RowType) rowType,
                            timestampFormat,
                            org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode.valueOf(
                                    mapNullKeyMode.name()),
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            writeNullProperties,
                            ignoreNullFields);
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
                    return tableSchemaInfo
                            .getSerializationSchema()
                            .serialize(
                                    tableSchemaInfo.getRowDataFromRecordData(
                                            dataChangeEvent.after(), RowKind.INSERT));
                case UPDATE:
                case REPLACE:
                    return tableSchemaInfo
                            .getSerializationSchema()
                            .serialize(
                                    tableSchemaInfo.getRowDataFromRecordData(
                                            dataChangeEvent.after(), RowKind.UPDATE_AFTER));
                case DELETE:
                    return null;
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
}
