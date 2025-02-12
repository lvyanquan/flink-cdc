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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** ITCase to test the evolving MySql CDC source. */
public class MySqlEvolvingParallelSourceITCase extends MySqlSourceTestBase {

    protected final UniqueDatabase customDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    @After
    public void cleanup() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadSingleTableWithMultipleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadMultipleTablesWithSingleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleTablesWithMultipleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                4,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlEvolvingParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverInBinlogPhase() throws Exception {
        testMySqlEvolvingParallelSource(
                FailoverType.TM, FailoverPhase.BINLOG, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testMySqlEvolvingParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInBinlogPhase() throws Exception {
        testMySqlEvolvingParallelSource(
                FailoverType.JM, FailoverPhase.BINLOG, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testMySqlEvolvingParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    protected void testMySqlEvolvingParallelSource(
            FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
            throws Exception {
        testMySqlEvolvingParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
    }

    protected void testMySqlEvolvingParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerTables)
            throws Exception {
        customDatabase.createAndInitialize();
        env.setParallelism(parallelism);
        // It's possible to trigger 2 failover because schema evolution coordinator may also trigger
        // global failover during the recovery.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 0));
        // Prepare and submit the pipeline
        tEnv.registerCatalog("mycat", new TestValuesCatalog("mycat", "mydb", false));
        tEnv.useCatalog("mycat");
        tEnv.executeSql(getSourceDDL(captureCustomerTables));
        // Keep the sink parallelism is different from the source parallelism to trigger shuffle.
        // This is because the KeyedUpsertingSinkFunction requires the records with same key should
        // belong to the same task. However, MySql CDC only promises the binlog records is late than
        // the snapshot records.
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "CREATE TABLE Sink WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'enable-schema-evolution' = 'true',\n"
                                        + "  'sink-insert-only' = 'false',\n"
                                        + "  'sink-changelog-mode-enforced' = 'I,UA,D',\n"
                                        + "  'sink.parallelism' = '%s') AS TABLE customers",
                                parallelism > 1 ? parallelism - 1 : 1));
        JobClient client =
                tableResult
                        .getJobClient()
                        .orElseThrow(() -> new TableException("Can not get client."));
        waitUntilJobRunning(client);
        // Trigger the failover before the binlog reading and validate
        JobID jobId = client.getJobID();
        if (failoverPhase == FailoverPhase.SNAPSHOT) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        List<String> expectedMaterializedDataDuringSnapshot = Arrays.asList(getSnapshottedRows());
        validateExpectedRows("Sink", expectedMaterializedDataDuringSnapshot);

        // wait binlog read starts
        Thread.sleep(5000);

        // Make change during the binlog phase
        Random random = new Random(System.currentTimeMillis());
        String tableId = captureCustomerTables[random.nextInt(captureCustomerTables.length)];
        addColumnsForSourceTable(getConnection(), customDatabase.getDatabaseName() + "." + tableId);
        if (failoverPhase == FailoverPhase.BINLOG) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        dropColumnsForSourceTable(
                getConnection(), customDatabase.getDatabaseName() + "." + tableId);

        List<String> expectedMaterializedDataDuringBinlog =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[103, user_3, Hangzhou, 123567891234, null]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Hangzhou, 123567891234, null]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]",
                        "+I[5001, flink-user, Hangzhou, 15957199999, 25]",
                        "+I[5002, cdc-user, Beijing, 15957187777, 30]",
                        "+I[5003, alibaba-user, Tokyo, 11122223333, 35]",
                        "+I[2001, user_22, null, 123567891234, 45]",
                        "+I[2002, user_23, null, 123567891234, 54]",
                        "+I[2003, user_24, null, 123567891234, 67]");
        waitUntilJobRunning(client);
        validateExpectedRows("Sink", expectedMaterializedDataDuringBinlog);
        tableResult.getJobClient().get().cancel().get();
    }

    private MySqlConnection getConnection() {
        MySqlSourceConfig sourceConfig =
                getConfig(customDatabase, new String[] {"customers"}, StartupOptions.initial());
        return DebeziumUtils.createMySqlConnection(sourceConfig);
    }

    private String getSourceDDL(String[] captureCustomerTables) {
        return String.format(
                "CREATE TABLE customers ("
                        + " id INT NOT NULL,"
                        + " name VARCHAR(255) NOT NULL,"
                        + " address VARCHAR(1024),"
                        + " phone_number VARCHAR(512),"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hostname' = '%s',"
                        + " 'port' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database-name' = '%s',"
                        + " 'table-name' = '%s',"
                        + " 'scan.incremental.snapshot.chunk.size' = '100',"
                        + " 'server-id' = '%s'"
                        + ")",
                MYSQL_CONTAINER.getHost(),
                MYSQL_CONTAINER.getDatabasePort(),
                customDatabase.getUsername(),
                customDatabase.getPassword(),
                customDatabase.getDatabaseName(),
                getTableName(captureCustomerTables),
                getServerId());
    }

    private String[] getSnapshottedRows() {
        return new String[] {
            "+I[101, user_1, Shanghai, 123567891234]",
            "+I[102, user_2, Shanghai, 123567891234]",
            "+I[103, user_3, Shanghai, 123567891234]",
            "+I[109, user_4, Shanghai, 123567891234]",
            "+I[110, user_5, Shanghai, 123567891234]",
            "+I[111, user_6, Shanghai, 123567891234]",
            "+I[118, user_7, Shanghai, 123567891234]",
            "+I[121, user_8, Shanghai, 123567891234]",
            "+I[123, user_9, Shanghai, 123567891234]",
            "+I[1009, user_10, Shanghai, 123567891234]",
            "+I[1010, user_11, Shanghai, 123567891234]",
            "+I[1011, user_12, Shanghai, 123567891234]",
            "+I[1012, user_13, Shanghai, 123567891234]",
            "+I[1013, user_14, Shanghai, 123567891234]",
            "+I[1014, user_15, Shanghai, 123567891234]",
            "+I[1015, user_16, Shanghai, 123567891234]",
            "+I[1016, user_17, Shanghai, 123567891234]",
            "+I[1017, user_18, Shanghai, 123567891234]",
            "+I[1018, user_19, Shanghai, 123567891234]",
            "+I[1019, user_20, Shanghai, 123567891234]",
            "+I[2000, user_21, Shanghai, 123567891234]"
        };
    }

    private static void validateExpectedRows(
            String sinkTableName, List<String> expectedMaterializedResults) throws Exception {
        Collections.sort(expectedMaterializedResults);
        List<String> actual = TestValuesTableFactory.getResults(sinkTableName);
        Deadline expireTime = Deadline.fromNow(Duration.ofSeconds(60));
        while (expireTime.hasTimeLeft()) {
            Collections.sort(actual);
            if (expectedMaterializedResults.equals(actual)) {
                return;
            }
            Thread.sleep(100);
            actual = TestValuesTableFactory.getResults(sinkTableName);
        }

        // make the last try
        actual = TestValuesTableFactory.getResults(sinkTableName);
        Collections.sort(actual);
        assertEquals(expectedMaterializedResults, actual);
    }

    private String getTableName(String[] captureCustomerTables) {
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return String.format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private void addColumnsForSourceTable(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            connection.execute(
                    "ALTER TABLE " + tableId + " ADD COLUMN age INT",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(5001, 'flink-user', 'Hangzhou', '15957199999', 25), "
                            + "(5002, 'cdc-user', 'Beijing', '15957187777', 30), "
                            + "(5003, 'alibaba-user', 'Tokyo', '11122223333', 35)");
            connection.commit();

            connection.execute(
                    "DELETE FROM " + tableId + " where id = 102",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void dropColumnsForSourceTable(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);
            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "ALTER TABLE " + tableId + " DROP COLUMN address",
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','123567891234', 45),"
                            + " (2002, 'user_23','123567891234', 54),"
                            + "(2003, 'user_24','123567891234', 67)");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void waitUntilJobRunning(JobClient client) throws Exception {
        while (!client.getJobStatus().get().equals(JobStatus.RUNNING)) {
            // Wait until job is running.
            Thread.sleep(100);
        }
    }
}
