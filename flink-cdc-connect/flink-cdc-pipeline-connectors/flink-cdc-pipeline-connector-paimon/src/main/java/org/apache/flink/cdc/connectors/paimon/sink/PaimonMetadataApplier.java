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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.paimon.sink.dlf.DlfCatalogUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.dlf.api.DlfDataToken;
import org.apache.flink.runtime.dlf.api.DlfResource;
import org.apache.flink.runtime.dlf.api.DlfResourceInfosCollector;
import org.apache.flink.runtime.dlf.api.VvrDataLakeConfig;
import org.apache.flink.util.Preconditions;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;
import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/**
 * A {@code MetadataApplier} that applies metadata changes to Paimon. Support primary key table
 * only.
 */
public class PaimonMetadataApplier implements MetadataApplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonMetadataApplier.class);

    // Catalog is unSerializable.
    private transient Catalog catalog;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Options catalogOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private final ReadableConfig flinkConf;

    public PaimonMetadataApplier(Options catalogOptions) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = new HashMap<>();
        this.partitionMaps = new HashMap<>();
        this.flinkConf = new Configuration();
    }

    public PaimonMetadataApplier(
            Options catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            ReadableConfig flinkConf) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.flinkConf = flinkConf;
    }

    @Override
    public synchronized String getToken(TableId tableId) {
        if (!isDlfCatalog()) {
            return null;
        }
        LOGGER.debug("Try to get token for " + tableId);
        try {
            tryGetDlfTablePermission(tableId.getSchemaName(), tableId.getTableName());
        } catch (Exception e) {
            LOGGER.error("Failed to get dlf table permission", e);
        }
        try {
            String endpoint =
                    Preconditions.checkNotNull(
                            catalogOptions.get(VvrDataLakeConfig.DLF_ENDPOINT),
                            String.format("%s cannot be null", VvrDataLakeConfig.DLF_ENDPOINT));
            String region =
                    Preconditions.checkNotNull(
                            catalogOptions.get(VvrDataLakeConfig.DLF_REGION),
                            String.format("%s cannot be null", VvrDataLakeConfig.DLF_REGION));
            // Get the data token and store it to the data token dir locally. This is required
            // during
            // sql planning.
            DlfDataToken token =
                    DlfResourceInfosCollector.getDataTokenRemotely(
                            flinkConf,
                            endpoint,
                            region,
                            DlfResource.builder()
                                    .catalogInstanceId(
                                            catalogOptions.get(
                                                    VvrDataLakeConfig.CATALOG_INSTANCE_ID))
                                    .databaseName(tableId.getSchemaName())
                                    .tableName(tableId.getTableName())
                                    .build());
            LOGGER.debug("Get table token: " + token + " for table:" + tableId);
            return token.toJson();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        Thread.currentThread().setContextClassLoader(PaimonMetadataApplier.class.getClassLoader());
        if (catalog == null) {
            DlfCatalogUtil.convertOptionToDlf(catalogOptions, flinkConf);
            catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
        }
        try {
            if (schemaChangeEvent instanceof CreateTableEvent) {
                applyCreateTable((CreateTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AddColumnEvent) {
                applyAddColumn((AddColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof DropColumnEvent) {
                applyDropColumn((DropColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof RenameColumnEvent) {
                applyRenameColumn((RenameColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
                applyAlterColumn((AlterColumnTypeEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof TruncateTableEvent) {
                applyTruncateTable((TruncateTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof DropTableEvent) {
                applyDropTable((DropTableEvent) schemaChangeEvent);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void applyCreateTable(CreateTableEvent event) throws Exception {
        tryGetDlfDatabasePermission(event.tableId().getSchemaName());
        if (!catalog.databaseExists(event.tableId().getSchemaName())) {
            catalog.createDatabase(event.tableId().getSchemaName(), true);
        }
        Schema schema = event.getSchema();
        org.apache.paimon.schema.Schema.Builder builder =
                new org.apache.paimon.schema.Schema.Builder();
        schema.getColumns()
                .forEach(
                        (column) ->
                                builder.column(
                                        column.getName(),
                                        LogicalTypeConversion.toDataType(
                                                DataTypeUtils.toFlinkDataType(column.getType())
                                                        .getLogicalType())));
        builder.primaryKey(schema.primaryKeys().toArray(new String[0]));
        if (partitionMaps.containsKey(event.tableId())) {
            List<String> primaryKeys = schema.primaryKeys();
            primaryKeys.addAll(partitionMaps.get(event.tableId()));
            builder.primaryKey(primaryKeys);
            builder.partitionKeys(partitionMaps.get(event.tableId()));
        } else if (schema.partitionKeys() != null && !schema.partitionKeys().isEmpty()) {
            builder.partitionKeys(schema.partitionKeys());
        }
        builder.options(tableOptions);
        builder.options(schema.options());
        catalog.createTable(tableIdToIdentifier(event), builder.build(), true);
        tryGetDlfTablePermission(event.tableId().getSchemaName(), event.tableId().getTableName());
    }

    private void applyAddColumn(AddColumnEvent event) throws Exception {
        List<SchemaChange> tableChangeList = applyAddColumnEventWithPosition(event);
        tryGetDlfTablePermission(event.tableId().getSchemaName(), event.tableId().getTableName());
        catalog.alterTable(tableIdToIdentifier(event), tableChangeList, true);
    }

    private List<SchemaChange> applyAddColumnEventWithPosition(AddColumnEvent event)
            throws Exception {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            SchemaChange tableChange;
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    tableChange =
                            SchemaChangeProvider.add(
                                    columnWithPosition,
                                    SchemaChange.Move.first(
                                            columnWithPosition.getAddColumn().getName()));
                    tableChangeList.add(tableChange);
                    break;
                case LAST:
                    SchemaChange schemaChangeWithLastPosition =
                            SchemaChangeProvider.add(columnWithPosition);
                    tableChangeList.add(schemaChangeWithLastPosition);
                    break;
                case BEFORE:
                    SchemaChange schemaChangeWithBeforePosition =
                            applyAddColumnWithBeforePosition(
                                    event.tableId().getSchemaName(),
                                    event.tableId().getTableName(),
                                    columnWithPosition);
                    tableChangeList.add(schemaChangeWithBeforePosition);
                    break;
                case AFTER:
                    checkNotNull(
                            columnWithPosition.getExistedColumnName(),
                            "Existing column name must be provided for AFTER position");
                    SchemaChange.Move after =
                            SchemaChange.Move.after(
                                    columnWithPosition.getAddColumn().getName(),
                                    columnWithPosition.getExistedColumnName());
                    tableChange = SchemaChangeProvider.add(columnWithPosition, after);
                    tableChangeList.add(tableChange);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown column position: " + columnWithPosition.getPosition());
            }
        }
        return tableChangeList;
    }

    private SchemaChange applyAddColumnWithBeforePosition(
            String schemaName,
            String tableName,
            AddColumnEvent.ColumnWithPosition columnWithPosition)
            throws Exception {
        String existedColumnName = columnWithPosition.getExistedColumnName();
        tryGetDlfTablePermission(schemaName, tableName);
        Table table = catalog.getTable(new Identifier(schemaName, tableName));
        List<String> columnNames = table.rowType().getFieldNames();
        int index = checkColumnPosition(existedColumnName, columnNames);
        SchemaChange.Move after =
                SchemaChange.Move.after(
                        columnWithPosition.getAddColumn().getName(), columnNames.get(index - 1));

        return SchemaChangeProvider.add(columnWithPosition, after);
    }

    private int checkColumnPosition(String existedColumnName, List<String> columnNames) {
        if (existedColumnName == null) {
            return 0;
        }
        int index = columnNames.indexOf(existedColumnName);
        checkArgument(index != -1, "Column %s not found", existedColumnName);
        return index;
    }

    private void applyDropColumn(DropColumnEvent event) throws Exception {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getDroppedColumnNames()
                .forEach((column) -> tableChangeList.add(SchemaChangeProvider.drop(column)));
        tryGetDlfTablePermission(event.tableId().getSchemaName(), event.tableId().getTableName());
        catalog.alterTable(tableIdToIdentifier(event), tableChangeList, true);
    }

    private void applyRenameColumn(RenameColumnEvent event) throws Exception {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getNameMapping()
                .forEach(
                        (oldName, newName) ->
                                tableChangeList.add(SchemaChangeProvider.rename(oldName, newName)));
        tryGetDlfTablePermission(event.tableId().getSchemaName(), event.tableId().getTableName());
        catalog.alterTable(tableIdToIdentifier(event), tableChangeList, true);
    }

    private void applyAlterColumn(AlterColumnTypeEvent event) throws Exception {
        List<SchemaChange> tableChangeList = new ArrayList<>();
        event.getTypeMapping()
                .forEach(
                        (oldName, newType) ->
                                tableChangeList.add(
                                        SchemaChangeProvider.updateColumnType(oldName, newType)));
        tryGetDlfTablePermission(event.tableId().getSchemaName(), event.tableId().getTableName());
        catalog.alterTable(tableIdToIdentifier(event), tableChangeList, true);
    }

    private void applyTruncateTable(TruncateTableEvent event) throws Exception {
        try (BatchTableCommit batchTableCommit =
                catalog.getTable(tableIdToIdentifier(event)).newBatchWriteBuilder().newCommit()) {
            batchTableCommit.truncateTable();
        }
    }

    private void applyDropTable(DropTableEvent event) throws SchemaEvolveException {
        try {
            catalog.dropTable(tableIdToIdentifier(event), true);
        } catch (Catalog.TableNotExistException e) {
            throw new SchemaEvolveException(event, "Failed to apply drop table event", e);
        }
    }

    private void tryGetDlfDatabasePermission(String database) throws Exception {
        if (isDlfCatalog()) {
            DlfResourceInfosCollector.collect(
                    flinkConf,
                    catalogOptions.toMap(),
                    DlfResource.builder()
                            .catalogInstanceId(
                                    catalogOptions.get(VvrDataLakeConfig.CATALOG_INSTANCE_ID))
                            .databaseName(database)
                            .build());
            LOGGER.debug("Succeed to get database permission for " + database);
        }
    }

    private void tryGetDlfTablePermission(String database, String tableName) throws Exception {
        if (isDlfCatalog()) {
            DlfResourceInfosCollector.collect(
                    flinkConf,
                    catalogOptions.toMap(),
                    DlfResource.builder()
                            .catalogInstanceId(
                                    catalogOptions.get(VvrDataLakeConfig.CATALOG_INSTANCE_ID))
                            .databaseName(database)
                            .tableName(tableName)
                            .build());
            LOGGER.debug("Succeed to get table permission for " + database + "." + tableName);
        }
    }

    private boolean isDlfCatalog() {
        return catalogOptions.containsKey("metastore")
                && catalogOptions.get("metastore").equals("dlf-paimon");
    }

    private static Identifier tableIdToIdentifier(SchemaChangeEvent event) {
        return new Identifier(event.tableId().getSchemaName(), event.tableId().getTableName());
    }
}
