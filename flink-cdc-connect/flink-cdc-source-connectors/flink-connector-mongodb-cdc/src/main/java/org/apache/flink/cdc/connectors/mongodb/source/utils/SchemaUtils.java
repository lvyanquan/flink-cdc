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

package org.apache.flink.cdc.connectors.mongodb.source.utils;

import org.apache.flink.cdc.connectors.mongodb.table.MongoDBReadableMetadata;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.data.ColumnSpec;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.AnsiLogicalTypeMerging;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class to deal schema. */
public class SchemaUtils {

    public static MetadataConverter[] getMetadataConverters(List<String> metadataKeys) {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(MongoDBReadableMetadata.values())
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(MongoDBReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }

    public static SchemaSpec mergeSchema(SchemaSpec schema1, SchemaSpec schema2) {
        Map<String, DataType> columns1 =
                schema1.getColumns().stream()
                        .collect(Collectors.toMap(ColumnSpec::getName, ColumnSpec::getDataType));
        Map<String, DataType> columns2 =
                schema2.getColumns().stream()
                        .collect(Collectors.toMap(ColumnSpec::getName, ColumnSpec::getDataType));
        List<String> columnNames1 = schema1.getColumnNames();
        List<String> columnNames2 = schema2.getColumnNames();

        SchemaSpec.Builder builder = SchemaSpec.newBuilder();
        for (String columnName : columnNames1) {
            if (columns2.containsKey(columnName)) {
                DataType type1 = columns1.get(columnName);
                DataType type2 = columns2.get(columnName);
                if (Objects.equals(type1, type2)) {
                    builder.column(columnName, type1);
                } else {
                    builder.column(
                            columnName,
                            findCommonDataType(columns1.get(columnName), columns2.get(columnName)));
                }
            } else {
                builder.column(columnName, columns1.get(columnName));
            }
        }
        for (String columnName : columnNames2) {
            if (!columns1.containsKey(columnName)) {
                builder.column(columnName, columns2.get(columnName));
            }
        }
        return builder.build();
    }

    private static DataType findCommonDataType(DataType... types) {
        Preconditions.checkArgument(types.length > 0, "List of types must not be empty.");
        Optional<LogicalType> commonLogicalType =
                AnsiLogicalTypeMerging.findCommonType(
                        Arrays.stream(types)
                                .map(DataType::getLogicalType)
                                .collect(Collectors.toList()));
        if (commonLogicalType.isPresent()) {
            return LogicalTypeDataTypeConverter.toDataType(commonLogicalType.get());
        }
        throw new IllegalStateException(
                "There must be a common data type to be found, some bug occurs.");
    }
}
