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

package org.apache.flink.cdc.services.conversion.utils;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.ColumnPositionDesc;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.DumperOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility methods for converting CXAS SQL to CDC YAML. */
public class ConvertingUtils {

    private ConvertingUtils() {}

    public static final String DEFAULT_REPLACE_SYMBOL = "<:>";

    public static String appendDdlAsComment(String originalDDL) {
        return "# Converted from the following SQL: \n# \n"
                + Arrays.stream(originalDDL.split("\n"))
                        .map(line -> "# " + line)
                        .collect(Collectors.joining("\n"))
                + "\n---\n";
    }

    public static String dumpYaml(String originalDdl, Object pipeline) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml yaml = new Yaml(options);
        return appendDdlAsComment(originalDdl) + yaml.dump(pipeline);
    }

    public static String quoteDotInRegexp(String str) {
        return str.replace(".", "\\.");
    }

    public static String generateTableId(@Nullable String database, @Nullable String table) {
        return Stream.of(database, table)
                .filter(Objects::nonNull)
                .map(ConvertingUtils::quoteDotInRegexp)
                .collect(Collectors.joining("."));
    }

    public static Map<String, Object> ordered(Map<String, ?> map) {
        Map<String, Object> orderedMap = new java.util.LinkedHashMap<>();
        map.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(e -> orderedMap.put(e.getKey(), e.getValue()));
        return orderedMap;
    }

    public static Map<String, String> extractHintsAsOptions(@Nullable SqlNodeList hints) {
        if (hints == null) {
            return Collections.emptyMap();
        }
        return hints.stream()
                .map(hint -> (SqlHint) hint)
                .flatMap(hint -> hint.getOptionKVPairs().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k1, k2) -> k1));
    }

    public static Map<String, String> extractPropertyListAsOptions(SqlNodeList propertyList) {
        return propertyList.stream()
                .map(option -> (SqlTableOption) option)
                .collect(
                        Collectors.toMap(
                                SqlTableOption::getKeyString,
                                SqlTableOption::getValueString,
                                (k1, k2) -> k1));
    }

    public static List<String> extractTableNames(SqlNodeList tableList) {
        return tableList.stream()
                .map(tbl -> (SqlCharStringLiteral) tbl)
                .map(SqlCharStringLiteral::toString)
                .map(str -> str.substring(1, str.length() - 1))
                .collect(Collectors.toList());
    }

    public static List<String> extractCalculatedColumnsExpression(
            @Nullable Map<SqlNode, ColumnPositionDesc> addedColumns) {
        if (addedColumns == null) {
            return Collections.emptyList();
        }

        return addedColumns.keySet().stream()
                .map(e -> (SqlTableColumn.SqlComputedColumn) e)
                .map(e -> e.getExpr().toString() + " AS " + e.getName())
                .collect(Collectors.toList());
    }

    public static List<String> extractPrimaryKeyConstraints(List<SqlTableConstraint> constraints) {
        return constraints.stream()
                .filter(SqlTableConstraint::isPrimaryKey)
                .flatMap(c -> c.getColumns().stream().map(SqlNode::toString))
                .collect(Collectors.toList());
    }

    public static List<String> extractPartitionKey(SqlNodeList partitionKeys) {
        return partitionKeys.stream()
                .map(node -> (SqlIdentifier) node)
                .map(id -> extractNamesFromIdentifier(id).get(0))
                .collect(Collectors.toList());
    }

    public static List<String> extractNamesFromIdentifier(Object identifier) {
        try {
            Class<?> clazz = identifier.getClass();
            Field field = clazz.getField("names");
            return (List<String>) field.get(identifier);
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to extract names from SqlIdentifier.", e);
        }
    }
}
