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

package org.apache.flink.cdc.connectors.sls.source.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SlsLogSchemaAwareDeserializationSchema} implementation using CONTINUOUS schema inference
 * strategy.
 */
public class SlsLogContinuousDeserializationSchema
        implements SlsLogSchemaAwareDeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;

    private final List<Event> reuseEventsToCollect;
    private final Map<TableId, TableSchemaHelper> tableSchemas;
    private final Set<TableId> alreadySendCreateTableTables;

    public SlsLogContinuousDeserializationSchema() {
        this.reuseEventsToCollect = new ArrayList<>();
        this.tableSchemas = new HashMap<>();
        this.alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public Event deserialize(FastLog fastLog) throws IOException {
        throw new RuntimeException(
                "Please invoke SlsLogDeserializationSchema#deserialize(FastLog, TableId, Collector<Event>) instead.");
    }

    @Override
    public void deserialize(FastLog fastLog, TableId tableId, Collector<Event> out)
            throws IOException {
        if (fastLog == null) {
            return;
        }
        reuseEventsToCollect.clear();

        // get schema for fast log
        Map<String, String> fieldValues = new HashMap<>();
        Schema.Builder builder = Schema.newBuilder();
        for (FastLogContent content : fastLog.getContents()) {
            fieldValues.put(content.getKey(), content.getValue());
            builder.physicalColumn(content.getKey(), DataTypes.STRING());
        }
        Schema schema = builder.build();

        // get schema change events if needed
        boolean tableSchemaExists = tableSchemas.containsKey(tableId);
        List<SchemaChangeEvent> schemaChanges = new ArrayList<>();
        if (tableSchemaExists) {
            Schema currentSchema = tableSchemas.get(tableId).schema;
            if (SchemaMergingUtils.isSchemaCompatible(currentSchema, schema)) {
                schema = currentSchema;
            } else {
                schema = SchemaMergingUtils.getLeastCommonSchema(currentSchema, schema);
                schemaChanges =
                        SchemaMergingUtils.getSchemaDifference(tableId, currentSchema, schema);
            }
        }

        // update schema if necessary
        TableSchemaHelper schemaHelper;
        if (!tableSchemaExists || !schemaChanges.isEmpty()) {
            schemaHelper = new TableSchemaHelper(schema);
            tableSchemas.put(tableId, schemaHelper);
        } else {
            schemaHelper = tableSchemas.get(tableId);
        }

        // send create table event if necessary
        if (!alreadySendCreateTableTables.contains(tableId)) {
            reuseEventsToCollect.add(new CreateTableEvent(tableId, schema));
            alreadySendCreateTableTables.add(tableId);
        } else {
            reuseEventsToCollect.addAll(schemaChanges);
        }

        // send data change event
        StringData[] rowFields = new StringData[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            String value = fieldValues.get(schema.getColumnNames().get(i));
            if (value != null) {
                rowFields[i] = new BinaryStringData(value);
            }
        }
        RecordData recordData = schemaHelper.generator.generate(rowFields);
        reuseEventsToCollect.add(DataChangeEvent.insertEvent(tableId, recordData));

        reuseEventsToCollect.forEach(out::collect);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (tableSchemas.containsKey(tableId)) {
            return tableSchemas.get(tableId).schema;
        }
        return null;
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        if (alreadySendCreateTableTables.contains(tableId)) {
            return;
        }
        if (tableSchemas.containsKey(tableId)) {
            schema =
                    SchemaMergingUtils.getLeastCommonSchema(
                            tableSchemas.get(tableId).schema, schema);
        }
        tableSchemas.put(tableId, new TableSchemaHelper(schema));
    }

    // --------------------------------------------------------------------------------------------

    static class TableSchemaHelper {
        Schema schema;
        BinaryRecordDataGenerator generator;

        TableSchemaHelper(Schema schema) {
            this.schema = schema;
            this.generator = new BinaryRecordDataGenerator((RowType) schema.toRowDataType());
        }
    }

    @VisibleForTesting
    Map<TableId, TableSchemaHelper> getTableSchemas() {
        return tableSchemas;
    }

    @VisibleForTesting
    public Set<TableId> getAlreadySendCreateTableTables() {
        return alreadySendCreateTableTables;
    }
}
