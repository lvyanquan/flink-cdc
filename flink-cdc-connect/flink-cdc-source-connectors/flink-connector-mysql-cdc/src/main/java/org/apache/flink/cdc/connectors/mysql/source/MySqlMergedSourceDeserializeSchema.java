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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.table.MySqlTableSpec;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.debezium.relational.Tables.TableFilter;
import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createTableFilter;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;

/**
 * The deserializer to deserialize the records from the Debezium to the records in the Flink. {@link
 * MySqlMergedSourceDeserializeSchema} should be used to deserialize records from a merged table
 * source (not an evolving source). It does not support schema evolution.
 */
public class MySqlMergedSourceDeserializeSchema
        implements DebeziumDeserializationSchema<SourceRecord> {
    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlMergedSourceDeserializeSchema.class);

    private final TypeInformation<SourceRecord> outputTypeInfo;
    private final Set<MySqlTableSpec> capturedTables;
    private final SimpleCollector collector;

    /** Use the filter to determine whether the input records belong to the table in Flink. */
    private transient List<Pair<TableFilter, MySqlTableSpec>> tableFiltersAndSpecs;

    /**
     * Cached mapping from the identity in Mysql to identity in Flink. One table in MySql may have
     * multiple identities in Flink. For example, the job may emit the records to two different
     * sinks(one needs system column and another doesn't).
     */
    private final Map<TableId, List<MySqlTableSpec>> cachedMySqlTableSpecs;
    /**
     * Serializer to deserialize the records. Although the table in MySql is same, their schema in
     * Flink maybe are different, which causes the deserializer is different.
     */
    private final Map<MySqlTableSpec, DebeziumDeserializationSchema<RowData>> serializers;

    private final Map<MySqlTableSpec, SchemaSpec> schemas;

    public MySqlMergedSourceDeserializeSchema(
            Set<MySqlTableSpec> capturedTables,
            Map<MySqlTableSpec, DebeziumDeserializationSchema<RowData>> deserializers,
            Map<MySqlTableSpec, SchemaSpec> schemas,
            TypeInformation<SourceRecord> outputTypeInfo) {
        this.capturedTables = capturedTables;
        this.outputTypeInfo = outputTypeInfo;
        this.cachedMySqlTableSpecs = new HashMap<>();
        this.serializers = deserializers;
        this.schemas = schemas;
        this.collector = new SimpleCollector();
    }

    @Override
    public void deserialize(
            org.apache.kafka.connect.source.SourceRecord record, Collector<SourceRecord> out)
            throws Exception {
        TableId tableId = getTableId(record);

        List<MySqlTableSpec> tableSpecs = getTableSpec(tableId);
        if (tableSpecs.isEmpty()) {
            LOG.warn("There's no object path for {}, so skip the record: {}.", tableId, record);
            return;
        }

        collector.innerCollector = out;
        for (MySqlTableSpec spec : tableSpecs) {
            if (!serializers.containsKey(spec)) {
                throw new TableException(
                        String.format(
                                "Don't find the serializer for the table: %s. "
                                        + "This may be caused by missing some schema change events. "
                                        + "This should never happen, please report a bug issue.",
                                tableId));
            }
            collector.tableSpec = spec;
            serializers.get(spec).deserialize(record, collector);
        }
    }

    private void initTableFilters() {
        tableFiltersAndSpecs = new ArrayList<>();
        // construct TableFilter for each ObjectPath
        for (MySqlTableSpec tableSpec : capturedTables) {
            TableFilter tableFilter =
                    createTableFilter(
                            tableSpec.getTablePathInMySql().getDatabaseName(),
                            tableSpec.getTablePathInMySql().getFullName());
            tableFiltersAndSpecs.add(Pair.of(tableFilter, tableSpec));
        }
    }

    @Override
    public TypeInformation<SourceRecord> getProducedType() {
        return outputTypeInfo;
    }

    private List<MySqlTableSpec> getTableSpec(TableId tableId) {
        if (tableFiltersAndSpecs == null) {
            initTableFilters();
        }
        if (!cachedMySqlTableSpecs.containsKey(tableId)) {
            cachedMySqlTableSpecs.put(
                    tableId,
                    tableFiltersAndSpecs.stream()
                            .filter(filterAndSpec -> filterAndSpec.getKey().isIncluded(tableId))
                            .map(Pair::getValue)
                            .collect(Collectors.toList()));
        }
        return cachedMySqlTableSpecs.getOrDefault(tableId, Collections.emptyList());
    }

    private class SimpleCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private transient Collector<SourceRecord> innerCollector;
        private transient MySqlTableSpec tableSpec;

        @Override
        public void collect(RowData record) {
            innerCollector.collect(
                    new SourceRecord(
                            tableSpec.getTablePathInFlink(), schemas.get(tableSpec), record));
        }

        @Override
        public void close() {}
    }
}
