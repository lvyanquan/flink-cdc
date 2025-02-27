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
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link SlsLogSchemaAwareDeserializationSchema} implementation using STATIC schema inference
 * strategy.
 */
public class SlsLogStaticDeserializationSchema
        implements SlsLogSchemaAwareDeserializationSchema<Event> {
    private static final long serialVersionUID = 1L;

    private final Map<TableId, TableSchemaHelper> tableSchemas;
    private final Set<TableId> alreadySendCreateTableTables;

    public SlsLogStaticDeserializationSchema() {
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
        if (!tableSchemas.containsKey(tableId)) {
            throw new IllegalStateException(
                    String.format("Unknown initial schema for table <%s>.", tableId));
        }
        if (fastLog == null) {
            return;
        }
        Map<String, String> fieldValues = new HashMap<>();
        for (FastLogContent content : fastLog.getContents()) {
            fieldValues.put(content.getKey(), content.getValue());
        }

        TableSchemaHelper helper = tableSchemas.get(tableId);
        Schema schema = helper.schema;
        StringData[] values = new StringData[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            String value = fieldValues.get(schema.getColumnNames().get(i));
            if (value != null) {
                values[i] = new BinaryStringData(value);
            }
        }
        DataChangeEvent event =
                DataChangeEvent.insertEvent(tableId, helper.generator.generate(values));
        if (!alreadySendCreateTableTables.contains(tableId)) {
            out.collect(new CreateTableEvent(tableId, schema));
            alreadySendCreateTableTables.add(tableId);
        }
        out.collect(event);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        if (!tableSchemas.containsKey(tableId)) {
            return null;
        }
        return tableSchemas.get(tableId).schema;
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        if (tableSchemas.containsKey(tableId)) {
            return;
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
    Set<TableId> getAlreadySendCreateTableTables() {
        return alreadySendCreateTableTables;
    }
}
