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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.schema.MySqlTypeUtils;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.cdc.connectors.mysql.table.MySqlDeserializationConverterFactory;
import org.apache.flink.cdc.connectors.mysql.table.MySqlReadableSystemColumn;
import org.apache.flink.cdc.connectors.mysql.table.MySqlTableSpec;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ColumnSpec;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.debezium.relational.Tables.TableFilter;
import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createTableFilter;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.mysql.source.utils.SerializerUtils.getMetadataConverters;
import static org.apache.flink.cdc.connectors.mysql.table.MySqlReadableMetadata.getMysqlReadableMetadata;

/**
 * The deserializer to deserialize the records from the Debezium to the records in the Flink. {@link
 * MySqlEvolvingSourceDeserializeSchema} also has the ability to apply the schema change when some
 * schema changes happens.
 *
 * <p>Note: The deserializer doesn't have any states. Therefore, we need to rely on the {@link
 * MySqlSourceReader} to restore the state during the recovery. When recovering, the reader needs to
 * re-initialize the deserializer to restore the schema of all captured tables.
 */
public class MySqlEvolvingSourceDeserializeSchema
        implements DebeziumDeserializationSchema<SourceRecord> {
    private static final Logger LOG =
            LoggerFactory.getLogger(MySqlEvolvingSourceDeserializeSchema.class);

    private static final TypeInformation<RowData> TYPEINFO_PLACE_HOLDER =
            TypeInformation.of(RowData.class);

    private final ZoneId serverTimeZone;
    private final MetadataConverter[] systemColumnConverters;
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
     * Cached serializer to deserialize the records. Although the table in MySql is same, their
     * schema in Flink maybe are different, which causes the deserializer is different.
     */
    private final Map<MySqlTableSpec, DebeziumDeserializationSchema<RowData>> cachedSerializer;
    /**
     * Cached schema to wrap the input records to {@link SourceRecord}. Although the table in MySql
     * is same, their schema in Flink maybe are different.
     */
    private final Map<MySqlTableSpec, SchemaSpec> cachedSchema;

    private final boolean treatTinyint1AsBool;

    private final boolean readChangelogAsAppend;

    public MySqlEvolvingSourceDeserializeSchema(
            Set<MySqlTableSpec> capturedTables,
            ZoneId serverTimeZone,
            TypeInformation<SourceRecord> outputTypeInfo,
            boolean treatTinyint1AsBool,
            boolean readChangelogAsAppend) {
        this.capturedTables = capturedTables;
        this.serverTimeZone = serverTimeZone;
        this.outputTypeInfo = outputTypeInfo;

        this.systemColumnConverters =
                Arrays.stream(MySqlReadableSystemColumn.values())
                        .map(MySqlReadableSystemColumn::getConverter)
                        .toArray(MetadataConverter[]::new);
        this.cachedMySqlTableSpecs = new HashMap<>();
        this.cachedSerializer = new HashMap<>();
        this.cachedSchema = new HashMap<>();
        this.collector = new SimpleCollector();

        this.treatTinyint1AsBool = treatTinyint1AsBool;
        this.readChangelogAsAppend = readChangelogAsAppend;
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
            if (!cachedSerializer.containsKey(spec)) {
                throw new TableException(
                        String.format(
                                "Don't find the serializer for the table: %s. "
                                        + "This may be caused by missing some schema change events. "
                                        + "This should never happen, please report a bug issue.",
                                tableId));
            }
            collector.tableSpec = spec;
            cachedSerializer.get(spec).deserialize(record, collector);
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

    public void applyTableChange(TableChanges.TableChange change) {
        switch (change.getType()) {
            case CREATE:
            case ALTER:
                registerOrUpdateSchema(change.getId(), change.getTable());
                break;
            case DROP:
                LOG.info("Receive drop table event from the source: {}. Ignore.", change);
                break;
            default:
                throw new IllegalArgumentException("Unknown change type: " + change.getType());
        }
    }

    @VisibleForTesting
    public void registerOrUpdateSchema(TableId tableId, Table table) {
        RowType physicalRowType =
                (RowType) MySqlTypeUtils.fromDbzTable(table, treatTinyint1AsBool).getLogicalType();
        List<MySqlTableSpec> tableSpecs = getTableSpec(tableId);

        for (MySqlTableSpec spec : tableSpecs) {
            boolean isShardingTable = spec.isShardingTable();
            RowDataDebeziumDeserializeSchema.Builder builder =
                    RowDataDebeziumDeserializeSchema.newBuilder();

            RowType rowTypeWithSystemColumns = physicalRowType;
            RowType actualPhysicalRowType = physicalRowType;

            // Skip update schema for sharding tables when there is already a wider schema in
            // cache
            SchemaSpec cachedTableSchema = cachedSchema.get(spec);
            List<String> metadataKeys = spec.getMetadataKeys();
            if (isShardingTable && cachedTableSchema != null) {
                if (cachedTableSchema
                        .getColumnNames()
                        .containsAll(
                                SchemaSpec.fromRowType(actualPhysicalRowType).getColumnNames())) {
                    LOG.info(
                            "Updating schema for table {} skipped, because there is a wider schema in other sharding tables.",
                            table.id().toQuotedString('`'));
                    continue;
                } else {
                    actualPhysicalRowType =
                            generateWiderRowType(
                                    cachedTableSchema, actualPhysicalRowType, metadataKeys);
                    LOG.info(
                            "Update schema for table {} in sharding tables to {}.",
                            table.id().toQuotedString('`'),
                            actualPhysicalRowType.asSummaryString());
                }
            }

            if (isShardingTable) {
                builder.setSystemColumnConverters(systemColumnConverters);
                rowTypeWithSystemColumns =
                        new RowType(
                                Stream.concat(
                                                Arrays.stream(MySqlReadableSystemColumn.values())
                                                        .map(
                                                                field ->
                                                                        new RowType.RowField(
                                                                                field.getKey(),
                                                                                field.getDataType()
                                                                                        .getLogicalType())),
                                                actualPhysicalRowType.getFields().stream())
                                        .collect(Collectors.toList()));
            }
            builder.setPhysicalRowType(actualPhysicalRowType)
                    .setResultTypeInfo(TYPEINFO_PLACE_HOLDER)
                    .setServerTimeZone(serverTimeZone)
                    .setUserDefinedConverterFactory(MySqlDeserializationConverterFactory.instance())
                    .setReadChangelogAsAppend(readChangelogAsAppend);

            if (metadataKeys != null && metadataKeys.size() > 0) {
                builder.setMetadataConverters(getMetadataConverters(metadataKeys));
                rowTypeWithSystemColumns =
                        addMetadataColumn(rowTypeWithSystemColumns, metadataKeys);
            }
            cachedSerializer.put(spec, builder.build());
            cachedSchema.put(spec, SchemaSpec.fromRowType(rowTypeWithSystemColumns));
        }
    }

    private RowType generateWiderRowType(
            SchemaSpec oldSchema, RowType newSchema, List<String> metadataKeys) {
        List<ColumnSpec> existedColumns = getOriginalColumns(oldSchema, metadataKeys);
        List<String> existedColumnNames =
                existedColumns.stream().map(ColumnSpec::getName).collect(Collectors.toList());

        List<RowType.RowField> fields = new ArrayList<>();
        for (ColumnSpec columnSpec : existedColumns) {
            fields.add(
                    new RowType.RowField(
                            columnSpec.getName(), columnSpec.getDataType().getLogicalType()));
        }
        for (RowType.RowField field : newSchema.getFields()) {
            if (!existedColumnNames.contains(field.getName())) {
                fields.add(field);
            }
        }
        return new RowType(fields);
    }

    private List<ColumnSpec> getOriginalColumns(SchemaSpec schema, List<String> metadataKeys) {
        List<ColumnSpec> columns = new ArrayList<>();
        List<String> systemColumnNames =
                Arrays.stream(MySqlReadableSystemColumn.values())
                        .map(MySqlReadableSystemColumn::getKey)
                        .collect(Collectors.toList());
        for (ColumnSpec column : schema.getColumns()) {
            String columnName = column.getName();
            if (systemColumnNames.contains(columnName) || metadataKeys.contains(columnName)) {
                continue;
            }
            columns.add(column);
        }
        return columns;
    }

    private RowType addMetadataColumn(RowType rowTypeWithSystemColumns, List<String> metadataKeys) {
        Stream<RowType.RowField> metadataFields =
                metadataKeys.stream()
                        .map(
                                key ->
                                        new RowType.RowField(
                                                key,
                                                getMysqlReadableMetadata(key)
                                                        .getDataType()
                                                        .getLogicalType()));
        return new RowType(
                Stream.concat(rowTypeWithSystemColumns.getFields().stream(), metadataFields)
                        .collect(Collectors.toList()));
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
                            tableSpec.getTablePathInFlink(), cachedSchema.get(tableSpec), record));
        }

        @Override
        public void close() {}
    }
}
