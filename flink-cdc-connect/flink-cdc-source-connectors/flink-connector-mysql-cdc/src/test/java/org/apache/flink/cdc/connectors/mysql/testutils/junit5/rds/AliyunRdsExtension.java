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

package org.apache.flink.cdc.connectors.mysql.testutils.junit5.rds;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTable;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTableSchemas;
import org.apache.flink.core.testutils.CommonTestUtils;

import com.aliyun.rds20140815.Client;
import com.aliyun.rds20140815.models.DescribeBinlogFilesRequest;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponse;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponseBody;
import com.aliyun.rds20140815.models.PurgeDBInstanceLogRequest;
import com.aliyun.rds20140815.models.PurgeDBInstanceLogResponse;
import com.aliyun.teaopenapi.models.Config;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JUnit 5 extension that load RDS related configurations from environment variable and prepare RDS
 * instance.
 */
public class AliyunRdsExtension implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunRdsExtension.class);

    // Environment variables of RDS specific connection configurations
    private static final String REGION_ID_ENV = "RDS_BINLOG_ARCHIVE_TEST_REGION_ID";
    private static final String ACCESS_KEY_ID_ENV = "RDS_BINLOG_ARCHIVE_TEST_ACCESS_KEY_ID";
    private static final String ACCESS_KEY_SECRET_ENV = "RDS_BINLOG_ARCHIVE_TEST_ACCESS_KEY_SECRET";
    private static final String DB_INSTANCE_ID_ENV = "RDS_BINLOG_ARCHIVE_TEST_DB_INSTANCE_ID";

    public static final String MAIN_DB_ID_ENV = "RDB_BINLOG_ARCHIVE_TEST_MAIN_DB_ID";

    // Environment variables of JDBC connections
    private static final String HOST_ENV = "RDS_BINLOG_ARCHIVE_TEST_HOST";
    private static final String PORT_ENV = "RDS_BINLOG_ARCHIVE_TEST_PORT";
    private static final String USERNAME_ENV = "RDS_BINLOG_ARCHIVE_TEST_USERNAME";
    private static final String PASSWORD_ENV = "RDS_BINLOG_ARCHIVE_TEST_PASSWORD";
    private static final String DATABASE_ENV = "RDS_BINLOG_ARCHIVE_TEST_DATABASE";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"));
    private static final Duration TIMEOUT = Duration.of(60, ChronoUnit.SECONDS);
    private static final int STATUS_CODE_SUCCESS = 200;

    private final MySqlSourceConfigFactory sourceConfigFactory;
    private final AliyunRdsConfig.RdsConfigBuilder rdsConfigBuilder;
    private final JDBCConnectionConfig jdbcConnectionConfig;
    private final MySqlConnection mySqlConnection;
    private final Client rdsClient;
    private final String randomizedCustomerTableName =
            "customers_" + RandomStringUtils.randomAlphanumeric(6);

    private TestTable customerTable;

    public AliyunRdsExtension() {
        this.rdsConfigBuilder = loadRdsConfigBuilderFromEnvVar();
        this.jdbcConnectionConfig = loadJdbcConfigFromEnvVar();
        this.sourceConfigFactory =
                new MySqlSourceConfigFactory()
                        .hostname(jdbcConnectionConfig.getHost())
                        .port(jdbcConnectionConfig.getPort())
                        .username(jdbcConnectionConfig.getUsername())
                        .password(jdbcConnectionConfig.getPassword())
                        .databaseList(jdbcConnectionConfig.getDatabase())
                        .tableList(
                                jdbcConnectionConfig.getDatabase()
                                        + "."
                                        + randomizedCustomerTableName)
                        .enableReadingRdsArchivedBinlog(rdsConfigBuilder.build());
        this.mySqlConnection =
                DebeziumUtils.createMySqlConnection(sourceConfigFactory.createConfig(0));
        this.rdsClient = initializeRDSClient(rdsConfigBuilder.build());
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        flushLogs();
        purgeLocalBinlog();
        customerTable =
                new TestTable(
                        jdbcConnectionConfig.getDatabase(),
                        randomizedCustomerTableName,
                        TestTableSchemas.CUSTOMERS);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        dropCustomerTable();
        createCustomerTable();
        insertIntoCustomerTable();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        dropCustomerTable();
        mySqlConnection.close();
    }

    // ---------------------------- Public Utilities ------------------------

    public MySqlSourceConfigFactory getSourceConfigFactory() {
        return sourceConfigFactory;
    }

    public MySqlConnection getMySqlConnection() {
        return mySqlConnection;
    }

    public AliyunRdsConfig.RdsConfigBuilder getRdsConfigBuilder() {
        return rdsConfigBuilder;
    }

    public JDBCConnectionConfig getJdbcConnectionConfig() {
        return jdbcConnectionConfig;
    }

    public TestTable getCustomerTable() {
        return customerTable;
    }

    public List<String> getArchivedBinlogFiles(long startTimestampMs, long stopTimestampMs)
            throws Exception {
        DescribeBinlogFilesRequest request = new DescribeBinlogFilesRequest();
        request.setStartTime(TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(startTimestampMs)));
        request.setEndTime(TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(stopTimestampMs)));
        request.setDBInstanceId(getRdsConfigBuilder().build().getDbInstanceId());
        DescribeBinlogFilesResponse response = rdsClient.describeBinlogFiles(request);
        if (response.statusCode != STATUS_CODE_SUCCESS) {
            throw new IllegalStateException(
                    String.format("Unexpected HTTP response code %s", response.statusCode));
        }
        return response.getBody().getItems().getBinLogFile().stream()
                .filter(
                        payload ->
                                payload.getHostInstanceID()
                                        .equals(getRdsConfigBuilder().build().getMainDbId()))
                .map(
                        DescribeBinlogFilesResponseBody
                                        .DescribeBinlogFilesResponseBodyItemsBinLogFile
                                ::getLogFileName)
                .sorted()
                .collect(Collectors.toList());
    }

    // ---------------------- Operations on customer table ---------------------

    public void createCustomerTable() throws Exception {
        mySqlConnection.execute(
                String.format("DROP TABLE IF EXISTS %s", customerTable.getTableId()));
        mySqlConnection.commit();
        mySqlConnection.execute(
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  id INTEGER NOT NULL PRIMARY KEY,\n"
                                + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                                + "  address VARCHAR(1024),\n"
                                + "  phone_number VARCHAR(512))",
                        customerTable.getTableId()));
        mySqlConnection.commit();
    }

    public void insertIntoCustomerTable() throws Exception {
        mySqlConnection.execute(
                String.format(
                        "INSERT INTO %s\n"
                                + "VALUES (101,\"user_1\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (102,\"user_2\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (103,\"user_3\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (109,\"user_4\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (110,\"user_5\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (111,\"user_6\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (118,\"user_7\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (121,\"user_8\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (123,\"user_9\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1009,\"user_10\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1010,\"user_11\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1011,\"user_12\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1012,\"user_13\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1013,\"user_14\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1014,\"user_15\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1015,\"user_16\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1016,\"user_17\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1017,\"user_18\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1018,\"user_19\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (1019,\"user_20\",\"Shanghai\",\"123567891234\"),\n"
                                + "       (2000,\"user_21\",\"Shanghai\",\"123567891234\")",
                        customerTable.getTableId()));
        mySqlConnection.commit();
    }

    public void clearCustomerTable() throws Exception {
        mySqlConnection.execute(
                String.format("DELETE FROM %s WHERE 1", customerTable.getTableId()));
        mySqlConnection.commit();
    }

    public void dropCustomerTable() throws Exception {
        String tableName = jdbcConnectionConfig.getDatabase() + "." + randomizedCustomerTableName;
        mySqlConnection.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
        mySqlConnection.commit();
    }

    public List<String> getExpectedCustomerTableRecords() {
        return Arrays.asList(
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
                "+I[2000, user_21, Shanghai, 123567891234]");
    }

    // ---------------------------- Helper functions -------------------------
    private void flushLogs() throws Exception {
        LOG.info("Flushing logs");
        mySqlConnection.execute("FLUSH LOGS");
        mySqlConnection.commit();
    }

    private void purgeLocalBinlog() throws Exception {
        LOG.info("Purging local binlog files");
        List<String> binlogFilesBeforePurge = mySqlConnection.availableBinlogFiles();
        LOG.info("Binlog files before purge: {}", binlogFilesBeforePurge);
        PurgeDBInstanceLogRequest request = new PurgeDBInstanceLogRequest();
        request.setDBInstanceId(getRdsConfigBuilder().build().getDbInstanceId());
        PurgeDBInstanceLogResponse response = rdsClient.purgeDBInstanceLog(request);
        checkState(
                STATUS_CODE_SUCCESS == response.getStatusCode(),
                "Unexpected response code %d: %s",
                response.getStatusCode(),
                response.getBody());
        CommonTestUtils.waitUtil(
                () -> mySqlConnection.availableBinlogFiles().size() <= 50,
                TIMEOUT,
                Duration.ofSeconds(2),
                String.format(
                        "Timeout waiting for binlog files to be purged. Current binlog files: %s",
                        mySqlConnection.availableBinlogFiles()));
        LOG.info(
                "Binlog files purging request sent. This may not be effective immediately. Remaining binlog files: {}",
                mySqlConnection.availableBinlogFiles());
    }

    private Client initializeRDSClient(AliyunRdsConfig rdsConfig) {
        Config config = new Config();
        config.setAccessKeyId(rdsConfig.getAccessKeyId());
        config.setAccessKeySecret(rdsConfig.getAccessKeySecret());
        config.setRegionId(rdsConfig.getRegionId());
        config.setReadTimeout(((int) TIMEOUT.toMillis()));
        config.setConnectTimeout(((int) TIMEOUT.toMillis()));
        try {
            return new Client(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to initialize RDS client", e);
        }
    }

    private static AliyunRdsConfig.RdsConfigBuilder loadRdsConfigBuilderFromEnvVar() {
        return AliyunRdsConfig.builder()
                .regionId(getEnvironmentVariable(REGION_ID_ENV))
                .accessKeyId(getEnvironmentVariable(ACCESS_KEY_ID_ENV))
                .accessKeySecret(getEnvironmentVariable(ACCESS_KEY_SECRET_ENV))
                .dbInstanceId(getEnvironmentVariable(DB_INSTANCE_ID_ENV))
                .mainDbId(getEnvironmentVariable(MAIN_DB_ID_ENV))
                .useIntranetLink(false);
    }

    private static JDBCConnectionConfig loadJdbcConfigFromEnvVar() {
        return new JDBCConnectionConfig(
                getEnvironmentVariable(HOST_ENV),
                Integer.parseInt(getEnvironmentVariable(PORT_ENV)),
                getEnvironmentVariable(USERNAME_ENV),
                getEnvironmentVariable(PASSWORD_ENV),
                getEnvironmentVariable(DATABASE_ENV));
    }

    private static String getEnvironmentVariable(String env) {
        String envValue = System.getenv(env);
        return checkNotNull(envValue, "Environment variable \"%s\" is required", env);
    }

    /** JDBC related configs. */
    public static class JDBCConnectionConfig {
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        private final String database;

        public JDBCConnectionConfig(
                String host, int port, String username, String password, String database) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.database = database;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDatabase() {
            return database;
        }
    }
}
