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

package org.apache.flink.cdc.services.conversion.expression;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.dumpYaml;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.generateTableId;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.ordered;

/** An intermediate representation of a YAML CDC job that could be constructed from a CTAS job. */
public class CtasExpression implements Expression {

    private final String sourceDatabase;
    private final String sourceTable;
    private final Map<String, String> sourceOptions;

    private final String sinkDatabase;
    private final String sinkTable;
    private final Map<String, String> sinkOptions;

    private final String originalDDL;
    private final List<String> calculatedColumns;
    private final List<String> primaryKeys;
    private final List<String> partitionKeys;

    public CtasExpression(
            String sourceDatabase,
            String sourceTable,
            Map<String, String> sourceOptions,
            String sinkDatabase,
            String sinkTable,
            Map<String, String> sinkOptions,
            String originalDDL,
            List<String> calculatedColumns,
            List<String> primaryKeys,
            List<String> partitionKeys) {
        this.sourceDatabase = sourceDatabase;
        this.sourceTable = sourceTable;
        this.sourceOptions = sourceOptions;
        this.sinkDatabase = sinkDatabase;
        this.sinkTable = sinkTable;
        this.sinkOptions = sinkOptions;
        this.originalDDL = originalDDL;
        this.calculatedColumns = calculatedColumns;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
    }

    @Override
    public String toString() {
        return "CtasExpression{"
                + "sourceDatabase='"
                + sourceDatabase
                + '\''
                + ", sourceTable='"
                + sourceTable
                + '\''
                + ", sourceOptions="
                + sourceOptions
                + ", sinkDatabase='"
                + sinkDatabase
                + '\''
                + ", sinkTable='"
                + sinkTable
                + '\''
                + ", sinkOptions="
                + sinkOptions
                + ", originalDDL='"
                + originalDDL
                + '\''
                + ", calculatedColumns="
                + calculatedColumns
                + ", primaryKeys="
                + primaryKeys
                + ", partitionKeys="
                + partitionKeys
                + '}';
    }

    @Override
    public String toYaml() {
        Map<String, String> sourceConfigMap = new HashMap<>(sourceOptions);
        Map<String, String> sinkConfigMap = new HashMap<>(sinkOptions);
        sourceConfigMap.put("tables", generateTableId(sourceDatabase, sourceTable));

        Map<String, Object> yamlMap = new LinkedHashMap<>();
        yamlMap.put("source", ordered(sourceConfigMap));
        yamlMap.put("sink", ordered(sinkConfigMap));
        yamlMap.put(
                "route",
                Collections.singletonList(
                        ordered(
                                ImmutableMap.of(
                                        "source-table",
                                                generateTableId(sourceDatabase, sourceTable),
                                        "sink-table", generateTableId(sinkDatabase, sinkTable)))));

        Map<String, String> transformMap = new LinkedHashMap<>();
        if (!calculatedColumns.isEmpty()) {
            transformMap.put("projection", "\\*, " + String.join(", ", calculatedColumns));
        }
        if (!primaryKeys.isEmpty()) {
            transformMap.put("primary-keys", String.join(",", primaryKeys));
        }
        if (!partitionKeys.isEmpty()) {
            transformMap.put("partition-keys", String.join(",", partitionKeys));
        }
        if (!transformMap.isEmpty()) {
            transformMap.put("source-table", generateTableId(sourceDatabase, sourceTable));
            yamlMap.put("transform", Collections.singletonList(transformMap));
        }

        return dumpYaml(originalDDL, yamlMap);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CtasExpression that = (CtasExpression) o;
        return Objects.equals(sourceDatabase, that.sourceDatabase)
                && Objects.equals(sourceTable, that.sourceTable)
                && Objects.equals(sourceOptions, that.sourceOptions)
                && Objects.equals(sinkDatabase, that.sinkDatabase)
                && Objects.equals(sinkTable, that.sinkTable)
                && Objects.equals(sinkOptions, that.sinkOptions)
                && Objects.equals(originalDDL, that.originalDDL)
                && Objects.equals(calculatedColumns, that.calculatedColumns)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(partitionKeys, that.partitionKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceDatabase,
                sourceTable,
                sourceOptions,
                sinkDatabase,
                sinkTable,
                sinkOptions,
                originalDDL,
                calculatedColumns,
                primaryKeys,
                partitionKeys);
    }

    /** Builder helper class to construct {@link CtasExpression}. */
    public static class Builder {
        private String sourceDatabase;
        private String sourceTable;
        private String sinkDatabase;
        private String sinkTable;
        private String originalDDL;
        private Map<String, String> sourceOptions = Collections.emptyMap();
        private Map<String, String> sinkOptions = Collections.emptyMap();
        private List<String> calculatedColumns = Collections.emptyList();
        private List<String> primaryKeys = Collections.emptyList();
        private List<String> partitionKeys = Collections.emptyList();

        public Builder setSourceDatabase(String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
            return this;
        }

        public Builder setSourceTable(String sourceTable) {
            this.sourceTable = sourceTable;
            return this;
        }

        public Builder setSourceOptions(Map<String, String> sourceOptions) {
            this.sourceOptions = sourceOptions;
            return this;
        }

        public Builder setSinkDatabase(String sinkDatabase) {
            this.sinkDatabase = sinkDatabase;
            return this;
        }

        public Builder setSinkTable(String sinkTable) {
            this.sinkTable = sinkTable;
            return this;
        }

        public Builder setSinkOptions(Map<String, String> sinkOptions) {
            this.sinkOptions = sinkOptions;
            return this;
        }

        public Builder setOriginalDDL(String originalDDL) {
            this.originalDDL = originalDDL;
            return this;
        }

        public Builder setCalculatedColumns(List<String> calculatedColumns) {
            this.calculatedColumns = calculatedColumns;
            return this;
        }

        public Builder setPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public Builder setPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public CtasExpression build() {
            return new CtasExpression(
                    sourceDatabase,
                    sourceTable,
                    sourceOptions,
                    sinkDatabase,
                    sinkTable,
                    sinkOptions,
                    originalDDL,
                    calculatedColumns,
                    primaryKeys,
                    partitionKeys);
        }
    }
}
