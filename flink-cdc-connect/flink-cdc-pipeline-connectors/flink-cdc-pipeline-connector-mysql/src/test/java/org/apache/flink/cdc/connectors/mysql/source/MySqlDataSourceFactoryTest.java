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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.OptionUtils.VVR_START_TIME_MS;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_ACCESS_KEY_ID;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_ACCESS_KEY_SECRET;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_BINLOG_DIRECTORIES_PARENT_PATH;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_BINLOG_DIRECTORY_PREFIX;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_DB_INSTANCE_ID;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_DOWNLOAD_TIMEOUT;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_REGION_ID;
import static org.apache.flink.cdc.connectors.mysql.factory.AliyunRdsOptions.RDS_USE_INTRANET_LINK;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.TABLES_EXCLUDE;
import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MySqlDataSourceFactory}. */
public class MySqlDataSourceFactoryTest extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", TEST_USER, TEST_PASSWORD);

    @Test
    public void testCreateSource() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                String.format("%s.prod\\.*:testCol", inventoryDatabase.getDatabaseName()));
        options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        MySqlSourceConfig sourceConfig = dataSource.getSourceConfig();
        assertThat(sourceConfig.getTableList())
                .isEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".products"));

        assertThat(sourceConfig.isScanNewlyAddedTableEnabled()).isEqualTo(true);
        assertThat(sourceConfig.getChunkKeyColumns().size()).isEqualTo(1);
        for (Map.Entry<ObjectPath, String> e : sourceConfig.getChunkKeyColumns().entrySet()) {
            assertThat(e.getKey())
                    .isEqualTo(new ObjectPath(inventoryDatabase.getDatabaseName(), "products"));
            assertThat(e.getValue()).isEqualTo("testCol");
        }
    }

    @Test
    public void testCreateSourceScanBinlogNewlyAddedTableEnabled() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put(SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        options.put(SCAN_NEWLY_ADDED_TABLE_ENABLED.key(), "true");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "If both scan.binlog.newly-added-table.enabled and scan.newly-added-table.enabled are true, data maybe duplicate after restore");
    }

    @Test
    public void testNoMatchedTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        String tables = inventoryDatabase.getDatabaseName() + ".test";
        options.put(TABLES.key(), tables);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    @Test
    public void testExcludeTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".\\.*");
        String tableExclude = inventoryDatabase.getDatabaseName() + ".orders";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isNotEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".orders"))
                .isEqualTo(
                        Arrays.asList(
                                inventoryDatabase.getDatabaseName() + ".customers",
                                inventoryDatabase.getDatabaseName() + ".multi_max_table",
                                inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    public void testExcludeAllTable() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        String tableExclude = inventoryDatabase.getDatabaseName() + ".prod\\.*";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tableExclude);
    }

    @Test
    public void testDatabaseAndTableWithTheSameName() throws SQLException {
        inventoryDatabase.createAndInitialize();
        // create a table with the same name of database
        try (Connection connection = inventoryDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            String createSameNameTableSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n"
                                    + "  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
                                    + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                                    + "  description VARCHAR(512)\n"
                                    + ");",
                            inventoryDatabase.getDatabaseName(),
                            inventoryDatabase.getDatabaseName());

            statement.execute(createSameNameTableSql);
        }
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(
                TABLES.key(),
                inventoryDatabase.getDatabaseName() + "." + inventoryDatabase.getDatabaseName());
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(
                        Arrays.asList(
                                inventoryDatabase.getDatabaseName()
                                        + "."
                                        + inventoryDatabase.getDatabaseName()));
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    public void testUnsupportedOption() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put("unsupported_key", "unsupported_value");

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'mysql'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testPrefixRequireOption() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put("jdbc.properties.requireSSL", "true");
        options.put("debezium.snapshot.mode", "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".products"));
    }

    @Test
    public void testAddChunkKeyColumns() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".\\.*");

        options.put(
                SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                inventoryDatabase.getDatabaseName()
                        + ".multi_max_\\.*:order_id;"
                        + inventoryDatabase.getDatabaseName()
                        + ".products:id;");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);

        ObjectPath multiMaxTable =
                new ObjectPath(inventoryDatabase.getDatabaseName(), "multi_max_table");
        ObjectPath productsTable = new ObjectPath(inventoryDatabase.getDatabaseName(), "products");

        assertThat(dataSource.getSourceConfig().getChunkKeyColumns())
                .isNotEmpty()
                .isEqualTo(
                        new HashMap<ObjectPath, String>() {
                            {
                                put(multiMaxTable, "order_id");
                                put(productsTable, "id");
                            }
                        });
    }

    @Test
    public void testSetStartTimeMs() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".\\.*");
        String tableExclude = inventoryDatabase.getDatabaseName() + ".orders";
        options.put(TABLES_EXCLUDE.key(), tableExclude);
        options.put(SCAN_STARTUP_MODE.key(), "earliest-offset");
        options.put(VVR_START_TIME_MS.key(), "1234");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getStartupOptions())
                .isEqualTo(StartupOptions.timestamp(1234L));
    }

    @Test
    public void testCreateSourceWithRdsConfig() {
        inventoryDatabase.createAndInitialize();
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), MYSQL_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        options.put(USERNAME.key(), TEST_USER);
        options.put(PASSWORD.key(), TEST_PASSWORD);
        options.put(TABLES.key(), inventoryDatabase.getDatabaseName() + ".prod\\.*");
        options.put(RDS_REGION_ID.key(), "1");
        options.put(RDS_ACCESS_KEY_ID.key(), "2");
        options.put(RDS_ACCESS_KEY_SECRET.key(), "3");
        options.put(RDS_DB_INSTANCE_ID.key(), "4");
        options.put(RDS_DOWNLOAD_TIMEOUT.key(), "100 s");
        options.put(RDS_BINLOG_DIRECTORIES_PARENT_PATH.key(), "fake-path");
        options.put(RDS_BINLOG_DIRECTORY_PREFIX.key(), "rds-p");
        options.put(RDS_USE_INTRANET_LINK.key(), "false");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        MySqlDataSourceFactory factory = new MySqlDataSourceFactory();
        MySqlDataSource dataSource = (MySqlDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList(inventoryDatabase.getDatabaseName() + ".products"));
        AliyunRdsConfig rdsConfig = dataSource.getSourceConfig().getRdsConfig();
        assertThat(rdsConfig.getRegionId()).isEqualTo("1");
        assertThat(rdsConfig.getAccessKeyId()).isEqualTo("2");
        assertThat(rdsConfig.getAccessKeySecret()).isEqualTo("3");
        assertThat(rdsConfig.getDbInstanceId()).isEqualTo("4");
        assertThat(rdsConfig.getDownloadTimeout()).isEqualTo(Duration.ofSeconds(100));
        assertThat(rdsConfig.isUseIntranetLink()).isEqualTo(false);
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
