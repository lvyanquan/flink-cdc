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

package org.apache.flink.cdc.connectors.hologres.sink;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.schema.HologresTypeHelper;
import org.apache.flink.cdc.connectors.hologres.sink.v2.HologresRecordSerializer;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.sink.v2.events.HologresSchemaFlushRecord;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.hologres.schema.HoloStatementUtils.DISTRIBUTION_KEY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypeHelper.inferTableSchema;

/**
 * A {@link HologresRecordSerializer} for converting {@link Event} into {@link Record} for {@link
 * com.alibaba.hologres.client.HoloClient}.
 */
@PublicEvolving
public class HologresRecordEventSerializer implements HologresRecordSerializer<Event> {
    /** keep the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    private final HologresConnectionParam param;

    public HologresRecordEventSerializer(HologresConnectionParam param) {
        tableInfoMap = new HashMap<>();
        this.param = param;
    }

    @Override
    public Record serialize(Event event) {
        if (event instanceof SchemaChangeEvent) {
            return applySchemaChangeEvent((SchemaChangeEvent) event);
        } else if (event instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) event);
        } else {
            throw new UnsupportedOperationException("Don't support event " + event);
        }
    }

    public HologresSchemaFlushRecord applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.schema, event);
        }

        TableInfo tableInfo = new TableInfo();
        tableInfo.schema = newSchema;
        String shardKeys = param.getTableOptions().get(DISTRIBUTION_KEY);
        TableSchema newHoloTableSchema =
                inferTableSchema(
                        tableInfo.schema, tableId, shardKeys, param.isEnableTypeNormalization());
        tableInfo.holoTableSchema = newHoloTableSchema;
        tableInfo.fieldGetters = new RecordData.FieldGetter[newSchema.getColumnCount()];
        for (int i = 0; i < newSchema.getColumnCount(); i++) {
            Column column = newSchema.getColumns().get(i);
            tableInfo.fieldGetters[i] =
                    HologresTypeHelper.createDataTypeToRecordFieldGetter(
                            column.getType(),
                            i,
                            newSchema.primaryKeys().contains(column.getName()));
        }
        tableInfoMap.put(tableId, tableInfo);
        return new HologresSchemaFlushRecord(newHoloTableSchema);
    }

    private Record applyDataChangeEvent(DataChangeEvent event) {
        TableInfo tableInfo = tableInfoMap.get(event.tableId());
        Preconditions.checkNotNull(tableInfo, event.tableId() + " is not existed");
        Record record = null;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                record = serializeRecord(tableInfo, event.after(), false);
                break;
            case DELETE:
                if (DeleteStrategy.DELETE_ROW_ON_PK.equals(param.getDeleteStrategy())) {
                    record = serializeRecord(tableInfo, event.before(), true);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }

        return record;
    }

    private Record serializeRecord(TableInfo tableInfo, RecordData recordData, boolean isDelete) {
        Record holoRecord = new Record(tableInfo.holoTableSchema);
        List<Column> columns = tableInfo.schema.getColumns();
        Preconditions.checkArgument(columns.size() == recordData.getArity());
        for (int index = 0; index < recordData.getArity(); index++) {
            Object fieldValue = tableInfo.fieldGetters[index].getFieldOrNull(recordData);
            if (fieldValue == null) {
                if (!param.isIgnoreNullWhenUpdate()) {
                    holoRecord.setObject(index, null);
                }
            } else {
                holoRecord.setObject(index, fieldValue);
            }
        }

        holoRecord.setType(isDelete ? Put.MutationType.DELETE : Put.MutationType.INSERT);
        return holoRecord;
    }

    /** Table information. */
    private static class TableInfo {
        Schema schema;

        TableSchema holoTableSchema;
        RecordData.FieldGetter[] fieldGetters;
    }
}
