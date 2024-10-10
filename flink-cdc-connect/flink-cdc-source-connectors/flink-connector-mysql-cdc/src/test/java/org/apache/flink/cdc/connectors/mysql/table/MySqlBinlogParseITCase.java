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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesEvolvingCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/** Integration tests for MySQL Table source. */
public class MySqlBinlogParseITCase extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");
    private final UniqueDatabase userDatabase1 =
            new UniqueDatabase(MYSQL8_CONTAINER, "user_1", TEST_USER, TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private StreamTableEnvironment tEnv;

    @BeforeClass
    public static void beforeClass() {
        LOG.info("Starting MySql8 containers...");
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        LOG.info("Container MySql8 is started.");
    }

    @AfterClass
    public static void afterClass() {
        LOG.info("Stopping MySql8 containers...");
        MYSQL8_CONTAINER.stop();
        LOG.info("Container MySql8 is stopped.");
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        tEnv = StreamTableEnvironment.create(env);

        tEnv.registerCatalog("mycat", new TestValuesEvolvingCatalog("mycat", "mydb", false));
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
    }

    @Test
    public void testParseBinlogError() throws Exception {
        userDatabase1.createAndInitialize();
        String sourceDDL =
                String.format(
                        "CREATE TABLE mysql_users ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " op_type STRING METADATA FROM 'row_kind' VIRTUAL,"
                                + " `id` DECIMAL(20, 0) NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " email STRING,"
                                + " age INT,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'server-id' = '%s',"
                                + " 'server-time-zone' = 'UTC',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        MYSQL8_CONTAINER.getHost(),
                        MYSQL8_CONTAINER.getDatabasePort(),
                        userDatabase1.getUsername(),
                        userDatabase1.getPassword(),
                        userDatabase1.getDatabaseName(),
                        "user_table_.*",
                        true,
                        getServerId(),
                        getSplitSize());

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " database_name STRING,"
                        + " table_name STRING,"
                        + " op_type STRING,"
                        + " `id` DECIMAL(20, 0) NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " email STRING,"
                        + " age INT,"
                        + " primary key (database_name, table_name, id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT * FROM mysql_users");

        // wait for snapshot finished and begin binlog
        waitForSinkSize("sink", 2);

        try (Connection connection = userDatabase1.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "CREATE TABLE `not_catched_table` (\n"
                            + "`id` int(11) NOT NULL AUTO_INCREMENT,\n"
                            + "`status_id` varchar(255) NOT NULL,\n"
                            + "`status_name` varchar(255),\n"
                            + "`create_time` datetime DEFAULT NULL COMMENT '创建时间',\n"
                            + "`update_time` datetime DEFAULT NULL COMMENT '更新时间',\n"
                            + "`modify_user_id` varchar(50) DEFAULT NULL COMMENT '修改用户id',\n"
                            + "`modify_user_name` varchar(50) DEFAULT NULL COMMENT '修改用户名称',\n"
                            + "PRIMARY KEY (`id`) USING BTREE\n"
                            + ") ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;");
            statement.execute(
                    "ALTER TABLE `not_catched_table`\n"
                            + "CHANGE COLUMN `id` `status_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' FIRST,\n"
                            + "CHANGE COLUMN `status_id` `status_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL AFTER `status_id`,\n"
                            + "CHANGE COLUMN `status_name` `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL AFTER `status_name`,\n"
                            + "DROP PRIMARY KEY,\n"
                            + "ADD PRIMARY KEY (`status_id`) USING BTREE;");
            statement.execute(
                    "INSERT INTO `not_catched_table`\n"
                            + "VALUES (111, 'test_name', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");

            statement.execute(
                    "INSERT INTO user_table_1_2 VALUES (200,'user_200','Wuhan',123567891234);");
            statement.execute(
                    "INSERT INTO user_table_1_1 VALUES (300,'user_300','Hangzhou',123567891234, 'user_300@foo.com');");
            statement.execute("UPDATE user_table_1_1 SET address='Beijing' WHERE id=300;");
            statement.execute("UPDATE user_table_1_2 SET phone_number=88888888 WHERE id=121;");
            statement.execute("DELETE FROM user_table_1_1 WHERE id=111;");
        }

        // waiting for binlog finished (5 more events)
        waitForSinkSize("sink", 9);

        List<String> expected =
                Stream.of(
                                "+I[%s, user_table_1_1, +I, 111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                                "+I[%s, user_table_1_2, +I, 121, user_121, Shanghai, 123567891234, null, null]",
                                "+I[%s, user_table_1_2, +I, 200, user_200, Wuhan, 123567891234, null, null]",
                                "+I[%s, user_table_1_1, +I, 300, user_300, Hangzhou, 123567891234, user_300@foo.com, null]",
                                "+U[%s, user_table_1_1, +U, 300, user_300, Beijing, 123567891234, user_300@foo.com, null]",
                                "+U[%s, user_table_1_2, +U, 121, user_121, Shanghai, 88888888, null, null]",
                                "-D[%s, user_table_1_1, -D, 111, user_111, Shanghai, 123567891234, user_111@foo.com, null]",
                                "-U[%s, user_table_1_1, -U, 300, user_300, Hangzhou, 123567891234, user_300@foo.com, null]",
                                "-U[%s, user_table_1_2, -U, 121, user_121, Shanghai, 123567891234, null, null]")
                        .map(s -> String.format(s, userDatabase1.getDatabaseName()))
                        .sorted()
                        .collect(Collectors.toList());

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        Collections.sort(actual);
        assertEquals(expected, actual);
        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testParseAlterSql() throws Exception {
        userDatabase1.createAndInitialize();

        try (Connection connection = userDatabase1.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE TABLE `test_source` (\n"
                            + "`id` int(11) NOT NULL AUTO_INCREMENT,\n"
                            + "`status_id` varchar(255) NOT NULL,\n"
                            + "`status_name` varchar(255),\n"
                            + "`create_time` datetime DEFAULT NULL COMMENT '创建时间',\n"
                            + "`update_time` datetime DEFAULT NULL COMMENT '更新时间',\n"
                            + "`modify_user_id` varchar(50) DEFAULT NULL COMMENT '修改用户id',\n"
                            + "`modify_user_name` varchar(50) DEFAULT NULL COMMENT '修改用户名称',\n"
                            + "PRIMARY KEY (`id`) USING BTREE\n"
                            + ") ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;");
            statement.execute("ALTER TABLE `test_source` AUTO_INCREMENT = 101;");
            statement.execute(
                    "INSERT INTO `test_source`\n"
                            + "VALUES (default, 'test_name1', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
        }

        // Build source
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .databaseList(userDatabase1.getDatabaseName())
                        .tableList(userDatabase1.getDatabaseName() + ".test_source")
                        .username(userDatabase1.getUsername())
                        .password(userDatabase1.getPassword())
                        .serverId("15401-15404")
                        .serverTimeZone("UTC")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.latest())
                        .includeSchemaChanges(true)
                        .build();

        // Build and execute the job
        DataStreamSource<String> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");
        try (CloseableIterator<String> iterator = source.executeAndCollect()) {
            Thread.sleep(4000L);
            try (Connection connection = userDatabase1.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES (default, 'test_name2', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "CHANGE COLUMN `id` `status_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' FIRST,\n"
                                + "CHANGE COLUMN `status_id` `status_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL AFTER `status_id`,\n"
                                + "CHANGE COLUMN `status_name` `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL AFTER `status_name`,\n"
                                + "DROP PRIMARY KEY,\n"
                                + "ADD PRIMARY KEY (`status_id`) USING BTREE;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES (default, 'test_name3', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "CHANGE COLUMN `status_id` `status_id1` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' FIRST,\n"
                                + "CHANGE COLUMN `status_name` `status_name1` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL AFTER `status_id1`,\n"
                                + "CHANGE COLUMN `description` `description1` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL AFTER `status_name1`;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES (default, 'test_name4', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "CHANGE COLUMN `status_id1` `status_id2` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' FIRST,\n"
                                + "RENAME COLUMN `status_name1` TO `status_name2`,\n"
                                + "RENAME COLUMN `description1` TO `description2`;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES (default, 'test_name5', 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "MODIFY COLUMN `status_id2` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' AFTER `status_name3`,\n"
                                + "CHANGE COLUMN `status_name2` `status_name3` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL FIRST,\n"
                                + "RENAME COLUMN `description2` TO `description3`;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES ('test_name6', default, 'test_desc', '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");

                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "CHANGE COLUMN `status_id2` `status_id3` int(11) NOT NULL AUTO_INCREMENT COMMENT '0:直播 1:超体 2:自习室 3:录播 4:回放 5:延伸课 6:common公共' AFTER `description3`;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES ('test_name7', 'test_desc', default, '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");

                statement.execute(
                        "ALTER TABLE `test_source`\n"
                                + "MODIFY COLUMN `description3` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL FIRST;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES ('test_desc', 'test_name8', default, '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");

                statement.execute(
                        "ALTER TABLE `test_source` RENAME COLUMN `status_name3` TO `status_name4`;");
                statement.execute(
                        "INSERT INTO `test_source`\n"
                                + "VALUES ('test_desc', 'test_name9', default, '2020-07-30 15:22:00', '2020-07-30 15:22:00', 'test_userid', 'test_username');");
            }

            List<String> expectedChangelogAfterStart =
                    Arrays.asList(
                            "{\"id\":102,\"status_id\":\"test_name2\",\"status_name\":\"test_desc\",\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"status_id\":103,\"status_name\":\"test_name3\",\"description\":\"test_desc\",\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"status_id1\":104,\"status_name1\":\"test_name4\",\"description1\":\"test_desc\",\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"status_id2\":105,\"status_name2\":\"test_name5\",\"description2\":\"test_desc\",\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"status_name3\":\"test_name6\",\"status_id2\":106,\"description3\":\"test_desc\",\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"status_name3\":\"test_name7\",\"description3\":\"test_desc\",\"status_id3\":107,\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"description3\":\"test_desc\",\"status_name3\":\"test_name8\",\"status_id3\":108,\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}",
                            "",
                            "{\"description3\":\"test_desc\",\"status_name4\":\"test_name9\",\"status_id3\":109,\"create_time\":1596122520000,\"update_time\":1596122520000,\"modify_user_id\":\"test_userid\",\"modify_user_name\":\"test_username\"}");

            List<String> rows = fetchRowData(iterator, expectedChangelogAfterStart.size());
            for (int i = 0; i < rows.size(); i++) {
                if (i % 2 == 0) {
                    assertEquals(expectedChangelogAfterStart.get(i), rows.get(i));
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------

    protected String getServerId(int base) {
        return base + "-" + (base + DEFAULT_PARALLELISM);
    }

    protected String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }

    private int getSplitSize() {
        return 4;
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private List<String> fetchRowData(Iterator<String> iter, int size) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            String row = iter.next();

            JsonNode jsonNode = objectMapper.readTree(row);
            if (jsonNode.has("after")) {
                JsonNode after = jsonNode.get("after");
                rows.add(after.toString());
                assertEquals("c", jsonNode.get("op").asText());
            } else {
                rows.add(row);
            }
            size--;
        }
        return rows;
    }
}
