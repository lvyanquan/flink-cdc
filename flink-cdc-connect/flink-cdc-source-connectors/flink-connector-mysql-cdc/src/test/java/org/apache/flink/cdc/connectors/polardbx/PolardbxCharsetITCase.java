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

package org.apache.flink.cdc.connectors.polardbx;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.StringUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;

/** Test supporting different column charsets for Polardbx. */
@RunWith(Parameterized.class)
public class PolardbxCharsetITCase extends PolardbxSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PolardbxCharsetITCase.class);

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(60);

    private static final String DDL_FILE = "charset_test";
    private static final String DATABASE_NAME = "cdc_c_" + getRandomSuffix();

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private StreamTableEnvironment tEnv;

    private final String testName;
    private final String[] snapshotExpected;
    private final String[] binlogExpected;

    public PolardbxCharsetITCase(
            String testName, String[] snapshotExpected, String[] binlogExpected) {
        this.testName = testName;
        this.snapshotExpected = snapshotExpected;
        this.binlogExpected = binlogExpected;
    }

    @Parameterized.Parameters(name = "Test column charset: {0}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {
                "utf8_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "+I[1, 测试数据]",
                    "+I[2, Craig Marshall]",
                    "+I[3, 另一个测试数据]",
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "ascii_test",
                new String[] {"+I[1, ascii test!?]", "+I[2, Craig Marshall]", "+I[3, {test}]"},
                new String[] {
                    "+I[1, ascii test!?]",
                    "+I[2, Craig Marshall]",
                    "+I[3, {test}]",
                    "-D[1, ascii test!?]",
                    "-D[2, Craig Marshall]",
                    "-D[3, {test}]",
                    "+I[11, ascii test!?]",
                    "+I[12, Craig Marshall]",
                    "+I[13, {test}]"
                }
            },
            new Object[] {
                "gbk_test",
                new String[] {"+I[1, 测试数据]", "+I[2, Craig Marshall]", "+I[3, 另一个测试数据]"},
                new String[] {
                    "+I[1, 测试数据]",
                    "+I[2, Craig Marshall]",
                    "+I[3, 另一个测试数据]",
                    "-D[1, 测试数据]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 另一个测试数据]",
                    "+I[11, 测试数据]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 另一个测试数据]"
                }
            },
            new Object[] {
                "latin1_test",
                new String[] {"+I[1, ÀÆÉ]", "+I[2, Craig Marshall]", "+I[3, Üæû]"},
                new String[] {
                    "+I[1, ÀÆÉ]",
                    "+I[2, Craig Marshall]",
                    "+I[3, Üæû]",
                    "-D[1, ÀÆÉ]",
                    "-D[2, Craig Marshall]",
                    "-D[3, Üæû]",
                    "+I[11, ÀÆÉ]",
                    "+I[12, Craig Marshall]",
                    "+I[13, Üæû]"
                }
            },
            new Object[] {
                "big5_test",
                new String[] {"+I[1, 大五]", "+I[2, Craig Marshall]", "+I[3, 丹店]"},
                new String[] {
                    "+I[1, 大五]",
                    "+I[2, Craig Marshall]",
                    "+I[3, 丹店]",
                    "-D[1, 大五]",
                    "-D[2, Craig Marshall]",
                    "-D[3, 丹店]",
                    "+I[11, 大五]",
                    "+I[12, Craig Marshall]",
                    "+I[13, 丹店]"
                }
            }
        };
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        initializePolardbxTables(
                DDL_FILE,
                DATABASE_NAME,
                s ->
                        !StringUtils.isNullOrWhitespaceOnly(s)
                                && (s.contains("utf8_test")
                                        || s.contains("latin1_test")
                                        || s.contains("gbk_test")
                                        || s.contains("big5_test")
                                        || s.contains("ascii_test")));
    }

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        env.setParallelism(4);
        env.enableCheckpointing(200);
    }

    @AfterClass
    public static void after() {
        dropDatabase(DATABASE_NAME);
    }

    @Test
    public void testCharset() throws Exception {
        String sourceDDL =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  table_id BIGINT,\n"
                                + "  table_name STRING,\n"
                                + "  primary key(table_id) not enforced"
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
                                + " 'server-time-zone' = 'Asia/Shanghai',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s'"
                                + ")",
                        testName,
                        getHost(),
                        PORT,
                        USER_NAME,
                        PASSWORD,
                        DATABASE_NAME,
                        testName,
                        true,
                        getServerId(),
                        4);
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(
                "CREATE TABLE sink ("
                        + "  table_id BIGINT,\n"
                        + "  table_name STRING,\n"
                        + "  primary key(table_id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")");
        // async submit job
        TableResult result =
                tEnv.executeSql(
                        String.format(
                                "INSERT INTO sink SELECT table_id,table_name FROM %s", testName));
        waitForSinkSize("sink", snapshotExpected.length);
        assertEqualsInAnyOrder(
                Arrays.asList(snapshotExpected), TestValuesTableFactory.getResults("sink"));

        // test binlog phase
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "/*TDDL:FORBID_EXECUTE_DML_ALL=FALSE*/UPDATE %s.%s SET table_id = table_id + 10;",
                            DATABASE_NAME, testName));
        }
        waitForSinkSize("sink", binlogExpected.length);
        LOG.info(
                "Polar charset test {}: {}",
                testName,
                String.join(",", TestValuesTableFactory.getRawResults("sink")));
        assertEqualsInAnyOrder(
                Arrays.asList(binlogExpected), TestValuesTableFactory.getRawResults("sink"));
        result.getJobClient().ifPresent(JobClient::cancel);
    }
}
