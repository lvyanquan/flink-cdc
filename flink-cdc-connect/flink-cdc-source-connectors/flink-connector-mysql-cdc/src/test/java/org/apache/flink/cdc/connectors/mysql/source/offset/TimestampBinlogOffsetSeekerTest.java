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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.SpecificStartingOffsetITCase;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTable;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTableSchemas;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for timestamp startup mode. */
class TimestampBinlogOffsetSeekerTest {

    private static final Logger LOG = LoggerFactory.getLogger(SpecificStartingOffsetITCase.class);
    @RegisterExtension static MiniClusterExtension miniCluster = new MiniClusterExtension();

    @SuppressWarnings("unchecked")
    private final MySqlContainer mysql =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride(
                                    buildMySqlConfigWithTimezone(
                                            getResourceFolder(), getSystemTimeZone()))
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(mysql, "customer", "mysqluser", "mysqlpw");
    private final TestTable customers =
            new TestTable(customDatabase, "customers", TestTableSchemas.CUSTOMERS);

    private MySqlConnection connection;

    @BeforeEach
    void prepare() throws Exception {
        mysql.start();
        connection = getConnection();
        customDatabase.createAndInitialize();
        flushLogs();
    }

    @AfterEach
    void tearDown() throws Exception {
        customDatabase.dropDatabase();
        connection.close();
        mysql.stop();
    }

    @Test
    void testSeekingToTimestampInTheFuture() {
        String expectedBinlogFile = DebeziumUtils.currentBinlogOffset(connection).getFilename();
        assertThat(
                        BinlogOffsetUtils.initializeEffectiveOffset(
                                BinlogOffset.ofTimestampSec(Long.MAX_VALUE / 1000),
                                connection,
                                getSourceConfig()))
                .isEqualTo(BinlogOffset.ofBinlogFilePosition(expectedBinlogFile, 0));
    }

    @Test
    void testSeekingToTimestampInThePast() {
        assertThat(
                        BinlogOffsetUtils.initializeEffectiveOffset(
                                BinlogOffset.ofTimestampSec(0L), connection, getSourceConfig()))
                .isEqualTo(BinlogOffset.ofEarliest());
    }

    @Test
    void testSeekingToTimestampInTheMiddle() throws Exception {
        // Insert new data
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15213, 'Alice', 'Rome', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15513, 'Bob', 'Milan', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (18213, 'Charlie', 'Paris', '123456987');",
                        customers.getTableId()));

        // Record current timestamp
        Thread.sleep(1000);
        long timestampMs = System.currentTimeMillis();
        BinlogOffset expected = DebeziumUtils.currentBinlogOffset(connection);

        // After recording the timestamp, insert some new data
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (19613, 'Tom', 'NewYork', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (20913, 'Cat', 'Washington', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (23313, 'Mouse', 'Seattle', '123456987');",
                        customers.getTableId()));

        BinlogOffset actual =
                BinlogOffsetUtils.initializeEffectiveOffset(
                        BinlogOffset.ofTimestampSec(timestampMs / 1000),
                        connection,
                        getSourceConfig());
        assertThat(actual.getFilename()).isEqualTo(expected.getFilename());
        assertThat(actual.getPosition()).isEqualTo(0);
    }

    private MySqlSourceConfig getSourceConfig() {
        return new MySqlSourceConfigFactory()
                .hostname(mysql.getHost())
                .port(mysql.getDatabasePort())
                .databaseList(customDatabase.getDatabaseName())
                .tableList("customers")
                .username(customDatabase.getUsername())
                .password(customDatabase.getPassword())
                .createConfig(0);
    }

    private MySqlConnection getConnection() {
        Configuration configuration = getDbzConfiguration();
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    private Configuration getDbzConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", mysql.getHost());
        properties.put("database.port", String.valueOf(mysql.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        properties.put("transaction.topic", "fake-topic");
        properties.put("database.server.name", "fake-name");
        return Configuration.from(properties);
    }

    private void executeStatements(String... statements) throws Exception {
        connection.execute(statements);
        connection.commit();
    }

    private void flushLogs() throws Exception {
        executeStatements("FLUSH LOGS;");
    }

    private static String buildMySqlConfigWithTimezone(File resourceDirectory, String timezone) {
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

    private static File getResourceFolder() {
        try {
            return Paths.get(
                            Objects.requireNonNull(
                                            SpecificStartingOffsetITCase.class
                                                    .getClassLoader()
                                                    .getResource("."))
                                    .toURI())
                    .toFile();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Get Resource File Directory fail");
        }
    }

    private static String getSystemTimeZone() {
        return ZoneId.systemDefault().toString();
    }
}
