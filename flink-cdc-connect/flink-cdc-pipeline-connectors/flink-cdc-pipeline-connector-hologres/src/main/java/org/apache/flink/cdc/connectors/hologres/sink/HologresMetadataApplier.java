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

package org.apache.flink.cdc.connectors.hologres.sink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.hologres.HologresJDBCClientProvider;
import org.apache.flink.cdc.connectors.hologres.schema.HoloStatementUtils;
import org.apache.flink.cdc.connectors.hologres.schema.HologresTableSchema;
import org.apache.flink.cdc.connectors.hologres.schema.HologresTypeHelper;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.utils.JDBCUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.cdc.connectors.hologres.schema.HoloStatementUtils.executeDDL;
import static org.apache.flink.cdc.connectors.hologres.schema.HoloStatementUtils.getQualifiedPath;
import static org.apache.flink.cdc.connectors.hologres.schema.HoloStatementUtils.preparePersistedOptionsStatement;

/** {@link MetadataApplier} for hologres. */
@Internal
public class HologresMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(HologresMetadataApplier.class);

    private final HologresConnectionParam param;

    private transient HologresJDBCClientProvider hologresJDBCClientProvider;

    public HologresMetadataApplier(HologresConnectionParam param) {
        this.param = param;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {

        LOG.info("hologres metadata applier receive schemaChangeEvent" + schemaChangeEvent);
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
        } else {
            throw new UnsupportedOperationException(
                    "HologresDataSink doesn't support schema change event " + schemaChangeEvent);
        }
        LOG.info(
                "hologres metadata applier success to apply schemaChangeEvent" + schemaChangeEvent);
    }

    private void applyCreateTable(CreateTableEvent event) {

        TableId tableId = event.tableId();
        try {
            HoloClient client = getHologresJDBCClientProvider().getClient();
            if (HoloStatementUtils.checkTableExists(client, tableId)) {
                LOG.warn(String.format("table %s is already exist", tableId));
                return;
            }

            HoloStatementUtils.createSchema(client, tableId.getSchemaName());
            Schema schema = event.getSchema();
            String tableDDL = prepareCreateTableStatement(tableId, schema, param.getTableOptions());
            executeDDL(client, tableDDL);

        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to create the table <%s>", tableId), e);
        } finally {
            hologresJDBCClientProvider.closeClient();
        }
    }

    // When added column in hologres, there are some limit:
    // 1. only can add column in the last position, thus ignore position information from
    // AddColumnEvent.
    // 2. not support add non-nullable column, thus will ignore non-nullable information from
    // AddColumnEvent.
    private void applyAddColumn(AddColumnEvent addColumnEvent) {
        TableId tableId = addColumnEvent.tableId();
        try {
            HologresTableSchema hologresTableSchema =
                    HologresTableSchema.get(
                            hologresJDBCClientProvider.getClient(), getQualifiedPath(tableId));
            List<String> addColumnList = new ArrayList<>();
            LinkedList<String> addColumnDdlList = new LinkedList<>();
            HoloClient client = getHologresJDBCClientProvider().getClient();
            List<AddColumnEvent.ColumnWithPosition> addedColumns = addColumnEvent.getAddedColumns();
            String pgTableName = getQualifiedPath(addColumnEvent.tableId());
            for (AddColumnEvent.ColumnWithPosition addColumn : addedColumns) {
                // not support computed/metadata column, just support physical type column
                if (!addColumn.getAddColumn().isPhysical()) {
                    throw new UnsupportedOperationException(
                            "Hologres metadata applier doesn't support alter table add computed or metadata column.");
                }

                String columnName = addColumn.getAddColumn().getName();
                if (hologresTableSchema.getColumn(columnName).isPresent()) {
                    LOG.info(
                            String.format("Column %s is already exist, just skip it.", columnName));
                    continue;
                }

                String addColumnDDL =
                        String.format(
                                "ADD COLUMN \"%s\" %s",
                                columnName,
                                HologresTypeHelper.toNullablePostgresType(
                                        addColumn.getAddColumn().getType(),
                                        false,
                                        param.isEnableTypeNormalization()));
                addColumnList.add(addColumnDDL);

                if (!StringUtils.isNullOrWhitespaceOnly(addColumn.getAddColumn().getComment())) {
                    String commentDDL =
                            String.format(
                                    "COMMENT ON COLUMN %s.\"%s\" IS '%s';",
                                    pgTableName, columnName, addColumn.getAddColumn().getComment());
                    addColumnDdlList.addLast(commentDDL);
                }
            }

            if (!addColumnList.isEmpty()) {
                String addColumnDDL =
                        String.format("ALTER TABLE %s", pgTableName)
                                + String.join(",", addColumnList)
                                + ";";
                addColumnDdlList.addFirst(addColumnDDL);
                String ddl =
                        String.format("BEGIN;\n%s\nCOMMIT;", String.join("\n", addColumnDdlList));
                executeDDL(client, ddl);
            }
        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            throw new FlinkRuntimeException(String.format("Failed to add column <%s>", tableId), e);
        } finally {
            hologresJDBCClientProvider.closeClient();
        }
    }

    private void applyDropColumn(DropColumnEvent dropColumnEvent) {
        // todo: Sink hologrs 2.0 support drop column action in beta , so not support in cdc now.
        throw new FlinkRuntimeException("Hologres not support drop columnType now");
    }

    private void applyRenameColumn(RenameColumnEvent renameColumnEvent) {

        List<String> renameColumnDDls = new ArrayList<>();
        TableId tableId = renameColumnEvent.tableId();
        String pgTableName = getQualifiedPath(renameColumnEvent.tableId());
        try {
            HoloClient client = getHologresJDBCClientProvider().getClient();
            HologresTableSchema hologresTableSchema =
                    HologresTableSchema.get(client, getQualifiedPath(tableId));
            Map<String, String> nameMappings = renameColumnEvent.getNameMapping();
            for (Map.Entry<String, String> nameMapping : nameMappings.entrySet()) {
                String nameBefore = nameMapping.getKey();
                String nameAfter = nameMapping.getValue();
                if (hologresTableSchema.getColumn(nameAfter).isPresent()) {
                    LOG.info(String.format("Column %s is already exist, just skip it.", nameAfter));
                    continue;
                }
                String renameColumnDDL =
                        String.format(
                                "ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\";",
                                pgTableName, nameBefore, nameAfter);
                renameColumnDDls.add(renameColumnDDL);
            }

            // This is crucial to execute in one transaction because the rename column operation in
            // Hologres is not atomic.
            // As a result, a single RenameColumnEvent might be interpreted as multiple rename
            // column operations.
            // If the first rename operation succeeds but subsequent ones fail, problem will occur.
            if (!renameColumnDDls.isEmpty()) {
                String ddl =
                        String.format("BEGIN;\n%s\nCOMMIT;", String.join("\n", renameColumnDDls));
                executeDDL(client, ddl);
            }
        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to rename column <%s>", tableId), e);
        }
    }

    private void applyAlterColumn(AlterColumnTypeEvent event) {
        // Comparing to decide whether to apply AlterColumnTypeEvent
        TableId tableId = event.tableId();
        try {
            HologresTableSchema tableSchema =
                    HologresTableSchema.get(
                            hologresJDBCClientProvider.getClient(), getQualifiedPath(tableId));
            boolean needApplyAlterColumn = false;
            for (Map.Entry<String, DataType> typeMapping : event.getTypeMapping().entrySet()) {
                Column currentColumn = tableSchema.getColumn(typeMapping.getKey()).get();
                Column columnAlter =
                        HologresTypeHelper.inferColumn(
                                typeMapping.getValue(), false, param.isEnableTypeNormalization());
                if (columnAlter.getArrayType() != currentColumn.getArrayType()
                        || columnAlter.getType() != currentColumn.getType()
                        || columnAlter.getTypeName() != currentColumn.getTypeName()) {
                    needApplyAlterColumn = true;
                    break;
                }
            }

            if (needApplyAlterColumn) {
                throw new FlinkRuntimeException("Hologres not support alter columnType now");
            } else {
                LOG.info("No need to apply idempotent AlterColumnTypeEvent: " + event);
            }
        } finally {
            hologresJDBCClientProvider.closeClient();
        }
    }

    String prepareCreateTableStatement(
            TableId tableId, Schema tableSchema, Map<String, String> tableOptions) {

        String tableDDL =
                HoloStatementUtils.prepareCreateTableStatement(
                        tableId,
                        tableSchema.getColumns(),
                        tableSchema.primaryKeys(),
                        tableSchema.partitionKeys(),
                        tableSchema.comment(),
                        true,
                        param.isEnableTypeNormalization());

        // get persisted options ddl
        String persistedOptionDDL = preparePersistedOptionsStatement(tableId, tableOptions);
        // the table ddl and persisted options ddl must be executed in one transaction
        return String.format("BEGIN;\n%s\n%s\nCOMMIT;", tableDDL, persistedOptionDDL);
    }

    private HologresJDBCClientProvider getHologresJDBCClientProvider() {
        if (hologresJDBCClientProvider != null) {
            return hologresJDBCClientProvider;
        }

        LOG.info(
                String.format(
                        "HologresMetadataApplier's endpoint is %s, database is %s, username is %s, tableOptions is %s",
                        param.getEndpoint(),
                        param.getDatabase(),
                        param.getUsername(),
                        param.getTableOptions()));
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(JDBCUtils.getDbUrl(param.getEndpoint(), param.getDatabase()));
        holoConfig.setUsername(param.getUsername());
        holoConfig.setPassword(param.getPassword());
        hologresJDBCClientProvider = new HologresJDBCClientProvider(holoConfig);

        return hologresJDBCClientProvider;
    }
}
