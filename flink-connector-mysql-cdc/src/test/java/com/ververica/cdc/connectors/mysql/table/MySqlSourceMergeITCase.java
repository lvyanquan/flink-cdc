/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.ExtendedMysqlConnectorConfig.SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED;
import static io.debezium.connector.mysql.ExtendedMysqlConnectorConfig.SCAN_PARALLEL_DESERIALIZE_CHANGELOG_ENABLED;
import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_MERGE_ENABLED;

/** ITCase to test the merged MySql CDC source. */
@RunWith(Parameterized.class)
public class MySqlSourceMergeITCase extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private static final String TEST_USER = "mysqluser";
    private static final String TEST_PASSWORD = "mysqlpw";

    protected final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", TEST_USER, TEST_PASSWORD);
    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    @Parameterized.Parameter(1)
    public boolean enableSourceMerge;

    @Parameterized.Parameters(name = "debeziumProperties: {0}, enableSourceMerge: {1}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {
                ImmutableMap.<String, String>builder()
                        .put(SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED.name(), "true")
                        .put(SCAN_PARALLEL_DESERIALIZE_CHANGELOG_ENABLED.name(), "true")
                        .build(),
                true
            },
            new Object[] {new HashMap<>(), false},
            new Object[] {new HashMap<>(), true}
        };
    }

    @After
    public void cleanup() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    public void testMergeSourceInTwoPipelines() throws Exception {
        tEnv.getConfig()
                .getConfiguration()
                .set(TABLE_OPTIMIZER_SOURCE_MERGE_ENABLED, enableSourceMerge);
        customerDatabase.createAndInitialize();
        inventoryDatabase.createAndInitialize();
        String source1DDL =
                String.format(
                        "CREATE TABLE source1 ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        getServerId());
        String sink1DDL =
                "CREATE TABLE sink1 ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(source1DDL);
        tEnv.executeSql(sink1DDL);

        String source2DDL =
                String.format(
                        "CREATE TABLE source2 ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " `id` INTEGER NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "customers",
                        getServerId());
        String sink2DDL =
                "CREATE TABLE sink2 ("
                        + " db_name STRING,"
                        + " table_name STRING,"
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(source2DDL);
        tEnv.executeSql(sink2DDL);

        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source1");
        statementSet.addInsertSql("INSERT INTO sink2 SELECT * FROM source2");
        TableResult result = statementSet.execute();

        waitForSinkSize("sink1", 9);
        waitForSinkSize("sink2", 21);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }
        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO customers VALUES (3000,'user_22','Beijing','123567891234')");
            statement.execute(
                    "UPDATE customers SET address='Hangzhou', phone_number='111111111111' WHERE id=101");
            statement.execute("DELETE FROM customers WHERE id=2000");
        }
        waitForSinkSize("sink1", 16);
        waitForSinkSize("sink2", 24);

        List<String> expectedSink1 =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140]",
                        "+I[102, car battery, 12V car battery, 8.100]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                        "+I[106, hammer, 16oz carpenter's hammer, 1.000]",
                        "+I[107, rocks, box of assorted rocks, 5.300]",
                        "+I[108, jacket, water resistent black wind breaker, 0.100]",
                        "+I[109, spare tire, 24 inch spare tire, 22.200]",
                        "+U[106, hammer, 18oz carpenter hammer, 1.000]",
                        "+U[107, rocks, box of assorted rocks, 5.100]",
                        "+I[110, jacket, water resistent white wind breaker, 0.200]",
                        "+I[111, scooter, Big 2-wheel scooter , 5.180]",
                        "+U[110, jacket, new water resistent white wind breaker, 0.500]",
                        "+U[111, scooter, Big 2-wheel scooter , 5.170]",
                        "-D[111, scooter, Big 2-wheel scooter , 5.170]");
        List<String> expectedSink2 =
                Stream.of(
                                "+I[%s, customers, 101, user_1, Shanghai, 123567891234]",
                                "+I[%s, customers, 102, user_2, Shanghai, 123567891234]",
                                "+I[%s, customers, 103, user_3, Shanghai, 123567891234]",
                                "+I[%s, customers, 109, user_4, Shanghai, 123567891234]",
                                "+I[%s, customers, 110, user_5, Shanghai, 123567891234]",
                                "+I[%s, customers, 111, user_6, Shanghai, 123567891234]",
                                "+I[%s, customers, 118, user_7, Shanghai, 123567891234]",
                                "+I[%s, customers, 121, user_8, Shanghai, 123567891234]",
                                "+I[%s, customers, 123, user_9, Shanghai, 123567891234]",
                                "+I[%s, customers, 1009, user_10, Shanghai, 123567891234]",
                                "+I[%s, customers, 1010, user_11, Shanghai, 123567891234]",
                                "+I[%s, customers, 1011, user_12, Shanghai, 123567891234]",
                                "+I[%s, customers, 1012, user_13, Shanghai, 123567891234]",
                                "+I[%s, customers, 1013, user_14, Shanghai, 123567891234]",
                                "+I[%s, customers, 1014, user_15, Shanghai, 123567891234]",
                                "+I[%s, customers, 1015, user_16, Shanghai, 123567891234]",
                                "+I[%s, customers, 1016, user_17, Shanghai, 123567891234]",
                                "+I[%s, customers, 1017, user_18, Shanghai, 123567891234]",
                                "+I[%s, customers, 1018, user_19, Shanghai, 123567891234]",
                                "+I[%s, customers, 1019, user_20, Shanghai, 123567891234]",
                                "+I[%s, customers, 2000, user_21, Shanghai, 123567891234]",
                                "+I[%s, customers, 3000, user_22, Beijing, 123567891234]",
                                "+U[%s, customers, 101, user_1, Hangzhou, 111111111111]",
                                "-D[%s, customers, 2000, user_21, Shanghai, 123567891234]")
                        .map(s -> String.format(s, customerDatabase.getDatabaseName()))
                        .collect(Collectors.toList());

        List<String> sink1Result = TestValuesTableFactory.getRawResults("sink1");
        List<String> sink2Result = TestValuesTableFactory.getRawResults("sink2");

        assertEqualsInAnyOrder(expectedSink1, sink1Result);
        assertEqualsInAnyOrder(expectedSink2, sink2Result);

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testMergeSourceInTwoPipelinesWithOpTypeAndAppendOnlyMode() throws Exception {
        tEnv.getConfig()
                .getConfiguration()
                .set(TABLE_OPTIMIZER_SOURCE_MERGE_ENABLED, enableSourceMerge);
        customerDatabase.createAndInitialize();
        inventoryDatabase.createAndInitialize();
        String source1DDL =
                String.format(
                        "CREATE TABLE source1 ("
                                + " `id` INT NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " op_type STRING METADATA FROM 'op_type' VIRTUAL,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'scan.read-changelog-as-append-only.enabled' = 'true',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        "products",
                        getServerId());
        String sink1DDL =
                "CREATE TABLE sink1 ("
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " description STRING,"
                        + " weight DECIMAL(10,3),"
                        + " op_type STRING,"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(source1DDL);
        tEnv.executeSql(sink1DDL);

        String source2DDL =
                String.format(
                        "CREATE TABLE source2 ("
                                + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                                + " table_name STRING METADATA VIRTUAL,"
                                + " `id` INTEGER NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " op_type STRING METADATA FROM 'op_type' VIRTUAL,"
                                + " primary key (`id`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mysql-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'scan.read-changelog-as-append-only.enabled' = 'true',"
                                + " 'server-id' = '%s'"
                                + ")",
                        MYSQL_CONTAINER.getHost(),
                        MYSQL_CONTAINER.getDatabasePort(),
                        customerDatabase.getUsername(),
                        customerDatabase.getPassword(),
                        customerDatabase.getDatabaseName(),
                        "customers",
                        getServerId());
        String sink2DDL =
                "CREATE TABLE sink2 ("
                        + " db_name STRING,"
                        + " table_name STRING,"
                        + " `id` INT NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " op_type STRING,"
                        + " primary key (`id`) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(source2DDL);
        tEnv.executeSql(sink2DDL);

        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 SELECT * FROM source1");
        statementSet.addInsertSql("INSERT INTO sink2 SELECT * FROM source2");
        TableResult result = statementSet.execute();

        waitForSinkSize("sink1", 9);
        waitForSinkSize("sink2", 21);

        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE products SET description='18oz carpenter hammer' WHERE id=106;");
            statement.execute("UPDATE products SET weight='5.1' WHERE id=107;");
            statement.execute(
                    "INSERT INTO products VALUES (default,'jacket','water resistent white wind breaker',0.2);"); // 110
            statement.execute(
                    "INSERT INTO products VALUES (default,'scooter','Big 2-wheel scooter ',5.18);");
            statement.execute(
                    "UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;");
            statement.execute("UPDATE products SET weight='5.17' WHERE id=111;");
            statement.execute("DELETE FROM products WHERE id=111;");
        }
        try (Connection connection = customerDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO customers VALUES (3000,'user_22','Beijing','123567891234')");
            statement.execute(
                    "UPDATE customers SET address='Hangzhou', phone_number='111111111111' WHERE id=101");
            statement.execute("DELETE FROM customers WHERE id=2000");
        }
        waitForSinkSize("sink1", 16);
        waitForSinkSize("sink2", 24);

        List<String> expectedSink1 =
                Arrays.asList(
                        "+I[101, scooter, Small 2-wheel scooter, 3.140, +I]",
                        "+I[102, car battery, 12V car battery, 8.100, +I]",
                        "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800, +I]",
                        "+I[104, hammer, 12oz carpenter's hammer, 0.750, +I]",
                        "+I[105, hammer, 14oz carpenter's hammer, 0.875, +I]",
                        "+I[106, hammer, 16oz carpenter's hammer, 1.000, +I]",
                        "+I[107, rocks, box of assorted rocks, 5.300, +I]",
                        "+I[108, jacket, water resistent black wind breaker, 0.100, +I]",
                        "+I[109, spare tire, 24 inch spare tire, 22.200, +I]",
                        "+I[106, hammer, 16oz carpenter's hammer, 1.000, -U]",
                        "+I[106, hammer, 18oz carpenter hammer, 1.000, +U]",
                        "+I[107, rocks, box of assorted rocks, 5.300, -U]",
                        "+I[107, rocks, box of assorted rocks, 5.100, +U]",
                        "+I[110, jacket, water resistent white wind breaker, 0.200, +I]",
                        "+I[111, scooter, Big 2-wheel scooter , 5.180, +I]",
                        "+I[110, jacket, water resistent white wind breaker, 0.200, -U]",
                        "+I[110, jacket, new water resistent white wind breaker, 0.500, +U]",
                        "+I[111, scooter, Big 2-wheel scooter , 5.180, -U]",
                        "+I[111, scooter, Big 2-wheel scooter , 5.170, +U]",
                        "+I[111, scooter, Big 2-wheel scooter , 5.170, -D]");
        List<String> expectedSink2 =
                Stream.of(
                                "+I[%s, customers, 101, user_1, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 102, user_2, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 103, user_3, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 109, user_4, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 110, user_5, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 111, user_6, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 118, user_7, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 121, user_8, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 123, user_9, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1009, user_10, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1010, user_11, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1011, user_12, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1012, user_13, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1013, user_14, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1014, user_15, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1015, user_16, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1016, user_17, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1017, user_18, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1018, user_19, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 1019, user_20, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 2000, user_21, Shanghai, 123567891234, +I]",
                                "+I[%s, customers, 3000, user_22, Beijing, 123567891234, +I]",
                                "+I[%s, customers, 101, user_1, Shanghai, 123567891234, -U]",
                                "+I[%s, customers, 101, user_1, Hangzhou, 111111111111, +U]",
                                "+I[%s, customers, 2000, user_21, Shanghai, 123567891234, -D]")
                        .map(s -> String.format(s, customerDatabase.getDatabaseName()))
                        .collect(Collectors.toList());

        List<String> sink1Result = TestValuesTableFactory.getRawResults("sink1");
        List<String> sink2Result = TestValuesTableFactory.getRawResults("sink2");

        assertEqualsInAnyOrder(expectedSink1, sink1Result);
        assertEqualsInAnyOrder(expectedSink2, sink2Result);

        result.getJobClient().get().cancel().get();
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
}
