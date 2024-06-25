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

package org.apache.flink.cdc.connectors.hologres.schema;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.util.StringUtils;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** An util that contains hologres statements such as DDL, DML and etc. */
public class HoloStatementUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HoloStatementUtils.class);
    private static final String DEFAULT_POSTGRES_SCHEMA_NAME = "public";

    // table properties
    public static final String BINLOG_LEVEL = "binlog.level";
    public static final String BINLOG_TTL = "binlog.ttl";
    public static final String DISTRIBUTION_KEY = "distribution_key";

    public static void executeDDL(HoloClient client, String ddl)
            throws HoloClientException, InterruptedException, ExecutionException {
        try {
            client.sql(
                            conn -> {
                                ConnectionUtil.refreshMeta(conn, 10000);
                                try (Statement stat = conn.createStatement()) {
                                    // Hologres V2.0 has optimized the syntax for using double
                                    // quotation marks ("") for escaping when setting table
                                    // attributes
                                    // https://help.aliyun.com/zh/hologres/user-guide/overview-3#section-dpy-3qa-8br
                                    stat.execute("set hg_disable_parse_holo_property = on;\n");
                                } catch (Exception ignored) {
                                    // ignore
                                }
                                try (Statement stat = conn.createStatement()) {
                                    // Hologres V2.0 will default to setting a maximum number of
                                    // shards for a single Table Group and a maximum number of
                                    // shards for the entire instance
                                    // https://help.aliyun.com/zh/hologres/user-guide/user-guide-of-table-groups-and-shard-counts#section-tw3-xvs-9ia
                                    stat.execute(
                                            "SET hg_experimental_enable_shard_count_cap=off;\n");
                                } catch (Exception ignored) {
                                    // ignore
                                }
                                try (Statement stat = conn.createStatement()) {
                                    stat.execute(ddl);
                                }
                                return null;
                            })
                    .get();
        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            LOG.error(String.format("Failed to execute SQL statement '%s' in Hologres", ddl), e);
            throw e;
        }
    }

    /**
     * @param fieldCount the number of fields to select.
     * @return a list of string array, each String[] corresponds to a row of select results
     */
    public static List<String[]> executeQuery(HoloClient client, String sql, int fieldCount)
            throws HoloClientException, InterruptedException, ExecutionException {
        List<String[]> res = new ArrayList<>();
        try {
            return client.sql(
                            conn -> {
                                ConnectionUtil.refreshMeta(conn, 10000);
                                try (Statement stat = conn.createStatement()) {
                                    ResultSet resultSet = stat.executeQuery(sql);
                                    while (resultSet.next()) {
                                        String[] row = new String[fieldCount];
                                        for (int i = 0; i < fieldCount; i++) {
                                            row[i] = resultSet.getString(i + 1);
                                        }
                                        res.add(row);
                                    }
                                    return res;
                                }
                            })
                    .get();
        } catch (HoloClientException | InterruptedException | ExecutionException e) {
            LOG.error(String.format("Failed to execute SQL statement '%s' in Hologres", sql), e);
            throw e;
        }
    }

    public static boolean checkTableExists(HoloClient client, TableId tableId)
            throws ExecutionException, InterruptedException, HoloClientException {
        List<String[]> result =
                executeQuery(
                        client,
                        String.format(
                                "select table_name from hologres.hg_table_properties where table_namespace='%s' and table_name = '%s';",
                                tableId.getSchemaName(), tableId.getTableName()),
                        1);
        return !result.isEmpty();
    }

    public static void dropTable(HoloClient client, TableId tableId)
            throws ExecutionException, InterruptedException, HoloClientException {
        executeDDL(client, String.format("DROP TABLE IF EXISTS %s;", getQualifiedPath(tableId)));
    }

    public static String prepareCreateTableStatement(
            TableId tableId,
            List<Column> columns,
            List<String> primaryKeys,
            List<String> partitionKeys,
            String tableComment,
            boolean ignoreIfNotExists,
            boolean enableTypeNormalization) {
        String qualifiedPath = getQualifiedPath(tableId);

        List<String> content =
                columns.stream()
                        .map(
                                column ->
                                        String.format(
                                                "  \"%s\" %s",
                                                column.getName(),
                                                HologresTypeHelper.toPostgresType(
                                                        column.getType(),
                                                        primaryKeys.contains(column.getName()),
                                                        enableTypeNormalization)))
                        .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            content.add(
                    String.format(
                            "PRIMARY KEY(%s)",
                            primaryKeys.stream()
                                    .map(col -> String.format("\"%s\"", col))
                                    .collect(Collectors.joining(","))));
        }

        String partitionStr = "";
        if (!partitionKeys.isEmpty()) {
            // hologres only support one partition key.
            String partitionKey = partitionKeys.get(0);
            if (!primaryKeys.contains(partitionKey)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "PRIMARY KEY constraint on table \"%s\" lacks column \"%s\" which is part of the partition key",
                                tableId, partitionKey));
            }
            partitionStr = String.format("PARTITION BY LIST (%s)", partitionKey);
        }

        List<String> comments = new ArrayList<>();
        if (!StringUtils.isNullOrWhitespaceOnly(tableComment)) {
            comments.add(
                    String.format("COMMENT ON TABLE %s IS '%s';", qualifiedPath, tableComment));
        }
        columns.forEach(
                column ->
                        Optional.ofNullable(column.getComment())
                                .ifPresent(
                                        columnComment ->
                                                comments.add(
                                                        String.format(
                                                                "COMMENT ON COLUMN %s.\"%s\" IS '%s';",
                                                                qualifiedPath,
                                                                column.getName(),
                                                                columnComment))));

        return String.format(
                "CREATE TABLE  %s %s(\n%s\n)\n%s;\n%s",
                ignoreIfNotExists ? "IF NOT EXISTS" : "",
                qualifiedPath,
                String.join(",\n", content),
                partitionStr,
                String.join("\n", comments));
    }

    public static void createSchema(HoloClient client, String schemaName)
            throws ExecutionException, InterruptedException, HoloClientException {
        String schemaDdl = String.format("CREATE SCHEMA IF NOT EXISTS \"%s\";", schemaName);
        executeDDL(client, schemaDdl);
    }

    public static String getQualifiedPath(TableId tableId) {
        if (tableId.getSchemaName().equals(DEFAULT_POSTGRES_SCHEMA_NAME)) {
            return String.format("\"%s\"", tableId.getTableName());
        } else {
            return String.format("\"%s\".\"%s\"", tableId.getSchemaName(), tableId.getTableName());
        }
    }

    public static String preparePersistedOptionsStatement(
            TableId tableId, Map<String, String> tableOptions) {
        String pgTableName = getQualifiedPath(tableId);

        List<String> persistedOptionKeys = new ArrayList<>(tableOptions.keySet());
        // Due to holo requires binlog.level must be set before setting binlog.ttl, so here if user
        // setting these two simultaneously, we should guarantee the order because of flink doesn't
        // guarantee it.
        if (persistedOptionKeys.contains(BINLOG_LEVEL)
                && persistedOptionKeys.contains(BINLOG_TTL)) {
            // remove it first
            persistedOptionKeys.remove(BINLOG_LEVEL);
            persistedOptionKeys.remove(BINLOG_TTL);

            // add these two keys to list last, BINLOG_LEVEL must before BINLOG_TTL
            persistedOptionKeys.add(BINLOG_LEVEL);
            persistedOptionKeys.add(BINLOG_TTL);
        }

        // The key need to remove prefix 'table_property.' that is used in flink
        return persistedOptionKeys.stream()
                .map(
                        key ->
                                String.format(
                                        "CALL SET_TABLE_PROPERTY('%s', '%s', '\"%s\"');",
                                        pgTableName, key, tableOptions.get(key)))
                .collect(Collectors.joining("\n"));
    }
}
