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
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.ExtendedMysqlConnectorConfig.SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED;
import static io.debezium.connector.mysql.ExtendedMysqlConnectorConfig.SCAN_PARALLEL_DESERIALIZE_CHANGELOG_ENABLED;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Basic class for testing {@link MySqlSource}. */
@RunWith(Parameterized.class)
public abstract class MySqlSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(MySqlSourceTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 4;
    protected static final MySqlContainer MYSQL_CONTAINER =
            createMySqlContainerWithGivenTimezone(getResourceFolder(), getSystemTimeZone());
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected InMemoryReporter metricReporter = InMemoryReporter.createWithRetainedMetrics();
    public static final String INTER_CONTAINER_MYSQL_ALIAS = "mysql";
    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .setConfiguration(
                                    metricReporter.addToConfiguration(new Configuration()))
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @Before
    public void prepare() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().set(TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS, true);
        env.enableCheckpointing(200L);
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
    }

    @Parameterized.Parameter() public Map<String, String> debeziumProperties;

    public Properties dbzProperties;

    @Parameterized.Parameters(name = "debeziumProperties: {0}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {
                ImmutableMap.<String, String>builder()
                        .put(SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED.name(), "true")
                        .put(SCAN_PARALLEL_DESERIALIZE_CHANGELOG_ENABLED.name(), "true")
                        .build()
            },
            new Object[] {new HashMap<>()}
        };
    }

    protected static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return createMySqlContainer(version, "docker/server-gtids/my.cnf");
    }

    protected static MySqlContainer createMySqlContainer(MySqlVersion version, String configPath) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride(configPath)
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName("flink-test")
                        .withUsername("flinkuser")
                        .withPassword("flinkpw")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    protected static MySqlContainer createMySqlContainerWithGivenTimezone(
            File resourceDirectory, String timezone) {
        return (MySqlContainer)
                new MySqlContainer()
                        .withConfigurationOverride(
                                buildMySqlConfigWithTimezone(resourceDirectory, timezone))
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName("flink-test")
                        .withUsername("flinkuser")
                        .withPassword("flinkpw")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_MYSQL_ALIAS)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    protected String getServerId() {
        return getServerId(DEFAULT_PARALLELISM);
    }

    protected String getServerId(int parallelism) {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + parallelism);
    }

    protected void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Get basic configuration.
     *
     * <p>TODO: Replace all similar usage in the community.
     */
    protected MySqlSourceConfig getConfig(
            UniqueDatabase customDatabase, String[] captureTables, StartupOptions startupOption) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customDatabase.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);
        return new MySqlSourceConfigFactory()
                .databaseList(customDatabase.getDatabaseName())
                .tableList(captureTableIds)
                .hostname(MYSQL_CONTAINER.getHost())
                .port(MYSQL_CONTAINER.getDatabasePort())
                .username(customDatabase.getUsername())
                .splitSize(10)
                .fetchSize(2)
                .password(customDatabase.getPassword())
                .startupOptions(startupOption)
                .debeziumProperties(dbzProperties)
                .createConfig(0);
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }

    public static void assertMapEquals(Map<String, ?> expected, Map<String, ?> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        for (String key : expected.keySet()) {
            assertEquals(expected.get(key), actual.get(key));
        }
    }

    public static String buildMySqlConfigWithTimezone(File resourceDirectory, String timezone) {
        try {
            TemporaryFolder tempFolder = new TemporaryFolder(resourceDirectory);
            tempFolder.create();
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
            String mysqldConf =
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n"
                            + "gtid_mode = on\n"
                            + "enforce_gtid_consistency = on\n";
            String timezoneConf = "default-time_zone = '" + timezone + "'\n";
            Files.write(
                    cnf,
                    Collections.singleton(mysqldConf + timezoneConf),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceDirectory.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }

    /** The type of failover. */
    protected enum FailoverType {
        TM,
        JM,
        NONE
    }

    /** The phase of failover. */
    protected enum FailoverPhase {
        SNAPSHOT,
        BINLOG,
        NEVER
    }

    protected static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    protected static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    protected static File getResourceFolder() {
        try {
            return Paths.get(
                            Objects.requireNonNull(
                                            MySqlSourceTestBase.class
                                                    .getClassLoader()
                                                    .getResource("."))
                                    .toURI())
                    .toFile();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Get Resource File Directory fail");
        }
    }

    protected static String getSystemTimeZone() {
        return ZoneId.systemDefault().toString();
    }

    protected static void waitForUpsertSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (upsertSinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected static int upsertSinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
