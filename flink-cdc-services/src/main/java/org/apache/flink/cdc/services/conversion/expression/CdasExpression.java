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
import java.util.stream.Collectors;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.DEFAULT_REPLACE_SYMBOL;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.dumpYaml;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.generateTableId;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.ordered;

/** An intermediate representation of a YAML CDC job that could be constructed from a CDAS job. */
public class CdasExpression implements Expression {

    private final String sourceDatabase;
    private final Map<String, String> sourceOptions;

    private final boolean includeAllTables;
    private final List<String> includedTables;
    private final List<String> excludedTables;

    private final String sinkDatabase;
    private final Map<String, String> sinkOptions;

    private final String originalDDL;

    public CdasExpression(
            String sourceDatabase,
            Map<String, String> sourceOptions,
            boolean includeAllTables,
            List<String> includedTables,
            List<String> excludedTables,
            String sinkDatabase,
            Map<String, String> sinkOptions,
            String originalDDL) {
        this.sourceDatabase = sourceDatabase;
        this.sourceOptions = sourceOptions;
        this.includeAllTables = includeAllTables;
        this.includedTables = includedTables;
        this.excludedTables = excludedTables;
        this.sinkDatabase = sinkDatabase;
        this.sinkOptions = sinkOptions;
        this.originalDDL = originalDDL;
    }

    @Override
    public String toYaml() {
        Map<String, String> sourceConfigMap = new HashMap<>(sourceOptions);
        Map<String, String> sinkConfigMap = new HashMap<>(sinkOptions);

        if (includeAllTables) {
            sourceConfigMap.put("tables", generateTableId(sourceDatabase, ".*"));
        } else {
            if (!includedTables.isEmpty()) {
                sourceConfigMap.put(
                        "tables",
                        includedTables.stream()
                                .map(table -> generateTableId(sourceDatabase, table))
                                .collect(Collectors.joining(",")));
            }
            if (!excludedTables.isEmpty()) {
                if (includedTables.isEmpty()) {
                    sourceConfigMap.put("tables", generateTableId(sourceDatabase, ".*"));
                }
                sourceConfigMap.put(
                        "tables.exclude",
                        excludedTables.stream()
                                .map(table -> generateTableId(sourceDatabase, table))
                                .collect(Collectors.joining(",")));
            }
        }

        // use LinkedHashMap to ensure certain orderliness
        Map<String, Object> yamlMap = new LinkedHashMap<>();
        yamlMap.put("source", ordered(sourceConfigMap));
        yamlMap.put("sink", ordered(sinkConfigMap));
        yamlMap.put(
                "route",
                Collections.singletonList(
                        ordered(
                                ImmutableMap.of(
                                        "source-table", generateTableId(sourceDatabase, ".*"),
                                        "sink-table",
                                                generateTableId(
                                                        sinkDatabase, DEFAULT_REPLACE_SYMBOL),
                                        "replace-symbol", DEFAULT_REPLACE_SYMBOL))));
        return dumpYaml(originalDDL, yamlMap);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CdasExpression that = (CdasExpression) o;
        return includeAllTables == that.includeAllTables
                && Objects.equals(sourceDatabase, that.sourceDatabase)
                && Objects.equals(sourceOptions, that.sourceOptions)
                && Objects.equals(includedTables, that.includedTables)
                && Objects.equals(excludedTables, that.excludedTables)
                && Objects.equals(sinkDatabase, that.sinkDatabase)
                && Objects.equals(sinkOptions, that.sinkOptions)
                && Objects.equals(originalDDL, that.originalDDL);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceDatabase,
                sourceOptions,
                includeAllTables,
                includedTables,
                excludedTables,
                sinkDatabase,
                sinkOptions,
                originalDDL);
    }

    /** Builder helper class to construct {@link CdasExpression}. */
    public static class Builder {
        private String sourceDatabase;
        private String sinkDatabase;
        private String originalDDL;

        private Map<String, String> sourceOptions = Collections.emptyMap();
        private Map<String, String> sinkOptions = Collections.emptyMap();

        private boolean includeAllTables = true;
        private List<String> includedTables = Collections.emptyList();
        private List<String> excludedTables = Collections.emptyList();

        public Builder setSourceDatabase(String sourceDatabase) {
            this.sourceDatabase = sourceDatabase;
            return this;
        }

        public Builder setSourceOptions(Map<String, String> sourceOptions) {
            this.sourceOptions = sourceOptions;
            return this;
        }

        public Builder setIncludeAllTables(boolean includeAllTables) {
            this.includeAllTables = includeAllTables;
            return this;
        }

        public Builder setIncludedTables(List<String> includedTables) {
            this.includedTables = includedTables;
            return this;
        }

        public Builder setExcludedTables(List<String> excludedTables) {
            this.excludedTables = excludedTables;
            return this;
        }

        public Builder setSinkDatabase(String sinkDatabase) {
            this.sinkDatabase = sinkDatabase;
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

        public CdasExpression build() {
            return new CdasExpression(
                    sourceDatabase,
                    sourceOptions,
                    includeAllTables,
                    includedTables,
                    excludedTables,
                    sinkDatabase,
                    sinkOptions,
                    originalDDL);
        }
    }
}
