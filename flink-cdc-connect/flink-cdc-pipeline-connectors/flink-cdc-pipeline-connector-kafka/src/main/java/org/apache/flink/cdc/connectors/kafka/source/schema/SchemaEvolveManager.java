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

package org.apache.flink.cdc.connectors.kafka.source.schema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@link SchemaEvolveManager} can get {@link SchemaEvolveResult} by current schema and new
 * schema. It detects new columns and find a common type for column in both schemas but with
 * different data type.
 */
public class SchemaEvolveManager {

    public static SchemaEvolveResult evolveSchema(
            TableId tableId, Schema currentSchema, Schema newSchema) {
        if (currentSchema.equals(newSchema)) {
            return SchemaEvolveResult.compatibleAsIs();
        }

        if (!hasSamePks(currentSchema, newSchema)) {
            return SchemaEvolveResult.incompatible("Primary keys should not be changed.");
        }

        List<Column> columnsToAdd =
                newSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .filter(col -> !currentSchema.getColumn(col.getName()).isPresent())
                        .collect(Collectors.toList());

        Map<String, Tuple2<DataType, DataType>> columnsToChangeType = new HashMap<>();
        for (Column column : currentSchema.getColumns()) {
            if (!column.isPhysical()) {
                continue;
            }
            Optional<Column> newColumn = newSchema.getColumn(column.getName());
            if (newColumn.isPresent()) {
                DataType newType = newColumn.get().getType();
                DataType curType = column.getType();
                if (!Objects.equals(curType, newType)) {
                    DataType commonType;
                    try {
                        commonType = DataTypeUtils.findCommonType(Arrays.asList(curType, newType));
                    } catch (Exception e) {
                        return SchemaEvolveResult.incompatible(
                                String.format(
                                        "Could not find common type for column <%s>, current data type is <%s>, new data type is <%s>",
                                        column.getName(), curType, newType));
                    }
                    if (!Objects.equals(curType, commonType)) {
                        columnsToChangeType.put(column.getName(), Tuple2.of(curType, commonType));
                    }
                }
            }
        }

        List<SchemaChangeEvent> schemaChanges = new ArrayList<>();
        Schema.Builder builder = Schema.newBuilder();
        builder.primaryKey(currentSchema.primaryKeys());

        currentSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (columnsToChangeType.containsKey(column.getName())) {
                                DataType newType = columnsToChangeType.get(column.getName()).f1;
                                builder.physicalColumn(
                                        column.getName(), newType, column.getComment());
                            } else {
                                builder.column(column);
                            }
                        });

        if (!columnsToChangeType.isEmpty()) {
            Map<String, DataType> beforeTypes = new HashMap<>();
            Map<String, DataType> afterTypes = new HashMap<>();
            columnsToChangeType.forEach(
                    (key, value) -> {
                        beforeTypes.put(key, value.f0);
                        afterTypes.put(key, value.f1);
                    });
            schemaChanges.add(new AlterColumnTypeEvent(tableId, afterTypes, beforeTypes));
        }
        if (!columnsToAdd.isEmpty()) {
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            columnsToAdd.forEach(
                    col -> {
                        builder.column(col);
                        // always add columns to the last
                        addedColumns.add(AddColumnEvent.last(col));
                    });
            schemaChanges.add(new AddColumnEvent(tableId, addedColumns));
        }

        return SchemaEvolveResult.compatibleAfterEvolution(builder.build(), schemaChanges);
    }

    /** Check whether two schemas have the primary keys (do not consider pk order). */
    private static boolean hasSamePks(Schema schema1, Schema schema2) {
        if (schema1.primaryKeys().equals(schema2.primaryKeys())) {
            return true;
        }
        Set<String> pks1 = new HashSet<>(schema1.primaryKeys());
        Set<String> pks2 = new HashSet<>(schema2.primaryKeys());
        return pks1.equals(pks2);
    }
}
