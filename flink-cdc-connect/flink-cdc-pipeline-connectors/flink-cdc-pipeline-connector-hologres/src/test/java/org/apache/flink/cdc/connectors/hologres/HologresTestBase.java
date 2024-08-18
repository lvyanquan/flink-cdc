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

package org.apache.flink.cdc.connectors.hologres;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.hologres.schema.HologresTypes;
import org.apache.flink.cdc.connectors.hologres.utils.JDBCUtils;
import org.apache.flink.table.api.EnvironmentSettings;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.org.postgresql.util.PSQLState;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;

/** Test configs. */
public class HologresTestBase {

    private static final String HOLOGRES_ENDPOINT = "VVR_TESTONLY_HOLOGRES_ENDPIOIT";
    private static final String HOLOGRES_USERNAME = "VVR_TESTONLY_HOLOGRES_USERNAME";
    private static final String HOLOGRES_PASSWORD = "VVR_TESTONLY_HOLOGRES_PASSWORD";

    public static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public final String randomSuffix = String.valueOf(Math.abs(new Random().nextLong()));

    public String endpoint;
    public String database;
    public String username;
    public String password;
    public String dimTable;
    public String sourceTable;
    public String sinkTable;

    public HologresTestBase() {

        database =
                "blink_defender_temp_"
                        + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        username = System.getenv(HOLOGRES_USERNAME);
        password = System.getenv(HOLOGRES_PASSWORD);
        endpoint = System.getenv(HOLOGRES_ENDPOINT);
        sourceTable = "test_source_table";
        dimTable = "test_dim_table";
        sinkTable = "test_sink_table";
    }

    public void cleanTable(String tableName) {
        executeSql("DELETE FROM " + tableName, true);
    }

    public void createTable(String createSql) {
        executeSql(createSql, false);
    }

    public void dropTable(String tableName) {
        executeSql(
                String.format(
                        "DROP TABLE IF EXISTS %s", TableName.valueOf("\"" + tableName + "\"")),
                false);
        executeSql(String.format("DROP TABLE IF EXISTS %s", TableName.valueOf(tableName)), true);
    }

    public boolean checkTableExists(String schemaName, String tableName) {

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet =
                    statement.executeQuery(
                            String.format(
                                    "select table_name from hologres.hg_table_properties where table_namespace='%s' and table_name = '%s';",
                                    schemaName, tableName));
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to check if the table <%s> exists", tableName), e);
        }
    }

    public boolean checkTableWithBinlog(String schemaName, String tableName) {
        if (!checkTableExists(schemaName, tableName)) {
            return false;
        }

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet =
                    statement.executeQuery(
                            String.format(
                                    "select table_name from hologres.hg_table_properties where table_namespace='%s' and table_name = '%s'"
                                            + " and property_key = 'binlog.level'"
                                            + " and property_value = 'replica'",
                                    schemaName, tableName));
            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Failed to check if the table <%s> is with binlog", tableName),
                    e);
        }
    }

    protected void executeSql(String sql, boolean ignoreException) {
        executeSql(sql, ignoreException, 60);
    }

    protected void executeSql(String sql, boolean ignoreException, int timeout) {
        executeSql(sql, ignoreException, timeout, database);
    }

    protected void executeSql(String sql, boolean ignoreException, int timeout, String dbName) {
        try (Connection connection = getConnection(dbName);
                Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(timeout);
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            if (PSQLState.QUERY_CANCELED.getState().equals(e.getSQLState())
                    && e.getMessage().contains("canceling statement due to user request")) {
                throw new RuntimeException(
                        String.format(
                                "failed to execute: %s because set query timeout %ss",
                                sql, timeout),
                        e);
            }
            if (!ignoreException) {
                throw new RuntimeException("Failed to execute: " + sql, e);
            }
        }
    }

    @Before
    @BeforeEach
    public void prepare() {
        EnvironmentSettings.Builder streamBuilder =
                EnvironmentSettings.newInstance().inStreamingMode();

        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Shanghai")));

        // connect exists db to create a new db
        executeSql("create database " + database, true, 60, "postgres");
        executeSql("create extension if not exists roaringbitmap;", false);
        executeSql("create extension if not exists hg_binlog;", false);
        executeSql(
                "alter database " + database + " set hg_experimental_enable_binlog_jsonb = on;",
                false);
        executeSql(
                "alter database " + database + " set hg_experimental_enable_shard_count_cap = off;",
                true);
        executeSql(
                "SET hg_experimental_enable_shard_count_cap=off; CALL HG_CREATE_TABLE_GROUP ('tg_1', 1);\n",
                true);
        executeSql("create schema if not exists test", false);
    }

    public Connection getConnection() throws SQLException {
        return getConnection(database);
    }

    public Connection getConnection(String dbName) throws SQLException {
        Connection conn =
                DriverManager.getConnection(
                        JDBCUtils.getDbUrl(endpoint, dbName), username, password);
        ConnectionUtil.refreshMeta(conn, 10000);
        return conn;
    }

    public HoloClient getHoloClient() throws HoloClientException {
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(JDBCUtils.getDbUrl(endpoint, database));
        config.setUsername(username);
        config.setPassword(password);
        return new HoloClient(config);
    }

    public static List<Tuple2<String, Integer>> holoTypes =
            Arrays.asList(
                    new Tuple2<>(HologresTypes.PG_SMALLINT, Types.SMALLINT),
                    new Tuple2<>(HologresTypes.PG_INTEGER, Types.INTEGER),
                    new Tuple2<>(HologresTypes.PG_BIGINT, Types.BIGINT),
                    new Tuple2<>(HologresTypes.PG_BOOLEAN, Types.BOOLEAN),
                    new Tuple2<>(HologresTypes.PG_REAL, Types.FLOAT),
                    new Tuple2<>(HologresTypes.PG_DOUBLE_PRECISION, Types.DOUBLE),
                    new Tuple2<>(HologresTypes.PG_NUMERIC, Types.DECIMAL),
                    new Tuple2<>(HologresTypes.PG_TEXT, Types.VARCHAR),
                    new Tuple2<>(HologresTypes.PG_JSON, Types.OTHER),
                    new Tuple2<>(HologresTypes.PG_JSONB, Types.OTHER),
                    new Tuple2<>(HologresTypes.PG_BYTEA, Types.BINARY),
                    new Tuple2<>(HologresTypes.PG_ROARING_BITMAP, Types.OTHER),
                    new Tuple2<>(HologresTypes.PG_TIME, Types.TIME),
                    new Tuple2<>(HologresTypes.PG_DATE, Types.DATE),
                    new Tuple2<>(HologresTypes.PG_TIMESTAMP, Types.TIMESTAMP),
                    new Tuple2<>(HologresTypes.PG_TIMESTAMPTZ, Types.TIMESTAMP_WITH_TIMEZONE));

    public static List<Tuple2<String, Integer>> holoArrayTypes =
            Arrays.asList(
                    new Tuple2<>(HologresTypes.PG_INTEGER_ARRAY, Types.ARRAY),
                    new Tuple2<>(HologresTypes.PG_BIGINT_ARRAY, Types.ARRAY),
                    new Tuple2<>(HologresTypes.PG_REAL_ARRAY, Types.ARRAY),
                    new Tuple2<>(HologresTypes.PG_DOUBLE_PRECISION_ARRAY, Types.ARRAY),
                    new Tuple2<>(HologresTypes.PG_BOOLEAN_ARRAY, Types.ARRAY),
                    new Tuple2<>(HologresTypes.PG_TEXT_ARRAY, Types.ARRAY));

    protected void executeBatchSql(List<String> sqlList, boolean ignoreException, int timeout) {
        for (String sql : sqlList) {
            executeSql(sql, ignoreException, timeout);
        }
    }

    public void executeBatchSql(
            String sqlFile, String schema, String tableName, boolean ignoreException)
            throws Exception {
        List<String> statements = loadSql(sqlFile, schema, tableName);
        executeSql("create schema if not exists " + schema, ignoreException);
        executeBatchSql(statements, ignoreException, 60);
    }

    protected List<String> loadSql(String sqlFile, String schema, String tableName)
            throws Exception {
        final String ddlFile = String.format("ddl/%s", sqlFile);
        final URL ddlTestFile = HologresTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        final List<String> statements =
                Arrays.stream(
                                Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                        .map(String::trim)
                                        .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                        .map(
                                                x -> {
                                                    final Matcher m = COMMENT_PATTERN.matcher(x);
                                                    return m.matches() ? m.group(1) : x;
                                                })
                                        .map(
                                                sql ->
                                                        sql.replace("$SCHEMA$", schema)
                                                                .replace("$TABLE$", tableName))
                                        .collect(Collectors.joining("\n"))
                                        .split("\n\n"))
                        .collect(Collectors.toList());
        return statements;
    }
}
