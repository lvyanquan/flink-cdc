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

package org.apache.flink.cdc.connectors.mysql.debezium;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/** Utilities related to Debezium. */
public class DebeziumUtils {
    private static final String QUOTED_CHARACTER = "`";

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumUtils.class);
    private static final long TIMEOUT_MS = 60000;

    /** Creates and opens a new {@link JdbcConnection} backing connection pool. */
    public static JdbcConnection openJdbcConnection(MySqlSourceConfig sourceConfig) {
        JdbcConnection jdbc =
                new JdbcConnection(
                        JdbcConfiguration.adapt(sourceConfig.getDbzConfiguration()),
                        new JdbcConnectionFactory(sourceConfig),
                        QUOTED_CHARACTER,
                        QUOTED_CHARACTER);
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open MySQL connection", e);
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    /** Creates a new {@link MySqlConnection}, but not open the connection. */
    public static MySqlConnection createMySqlConnection(MySqlSourceConfig sourceConfig) {
        return createMySqlConnection(
                sourceConfig.getDbzConfiguration(), sourceConfig.getJdbcProperties());
    }

    /** Creates a new {@link MySqlConnection}, but not open the connection. */
    public static MySqlConnection createMySqlConnection(
            Configuration dbzConfiguration, Properties jdbcProperties) {
        return new MySqlConnection(
                new MySqlConnection.MySqlConnectionConfiguration(dbzConfiguration, jdbcProperties));
    }

    /** Creates a new {@link BinaryLogClient} for consuming mysql binlog. */
    public static BinaryLogClient createBinaryClient(Configuration dbzConfiguration) {
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dbzConfiguration);
        return new BinaryLogClient(
                connectorConfig.hostname(),
                connectorConfig.port(),
                connectorConfig.username(),
                connectorConfig.password());
    }

    /** Creates a new {@link MySqlDatabaseSchema} to monitor the latest MySql database schemas. */
    public static MySqlDatabaseSchema createMySqlDatabaseSchema(
            MySqlConnectorConfig dbzMySqlConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(dbzMySqlConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(dbzMySqlConfig);
        return new MySqlDatabaseSchema(
                dbzMySqlConfig,
                valueConverters,
                topicSelector,
                schemaNameAdjuster,
                isTableIdCaseSensitive);
    }

    /** Fetch current binlog offsets in MySql Server. */
    public static BinlogOffset currentBinlogOffset(JdbcConnection jdbc) {
        final String showMasterStmt = "SHOW MASTER STATUS";
        try {
            return jdbc.queryAndMap(
                    showMasterStmt,
                    rs -> {
                        if (rs.next()) {
                            final String binlogFilename = rs.getString(1);
                            final long binlogPosition = rs.getLong(2);
                            final String gtidSet =
                                    rs.getMetaData().getColumnCount() > 4 ? rs.getString(5) : null;
                            return BinlogOffset.builder()
                                    .setBinlogFilePosition(binlogFilename, binlogPosition)
                                    .setGtidSet(gtidSet)
                                    .build();
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the binlog filename and position via '"
                                            + showMasterStmt
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the binlog filename and position via '"
                            + showMasterStmt
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    /** Create a TableFilter by database name and table name. */
    public static Tables.TableFilter createTableFilter(String database, String table) {
        final Selectors.TableSelectionPredicateBuilder eligibleTables =
                Selectors.tableSelector().includeDatabases(database);

        Predicate<TableId> tablePredicate = eligibleTables.includeTables(table).build();

        Predicate<TableId> finalTablePredicate =
                tablePredicate.and(
                        Tables.TableFilter.fromPredicate(MySqlConnectorConfig::isNotBuiltInTable)
                                ::isIncluded);
        return finalTablePredicate::test;
    }

    // --------------------------------------------------------------------------------------------

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig dbzMySqlConfig) {
        TemporalPrecisionMode timePrecisionMode = dbzMySqlConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = dbzMySqlConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                dbzMySqlConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
                dbzMySqlConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                dbzMySqlConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }

    public static List<TableId> discoverCapturedTables(
            JdbcConnection jdbc, MySqlSourceConfig sourceConfig) {

        final List<TableId> capturedTableIds;
        try {
            capturedTableIds =
                    TableDiscoveryUtils.listTables(
                            jdbc, sourceConfig.getDatabaseFilter(), sourceConfig.getTableFilter());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            sourceConfig.getDatabaseList(), sourceConfig.getTableList()));
        }
        return capturedTableIds;
    }

    public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
        return !"0"
                .equals(
                        readMySqlSystemVariables(connection)
                                .get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    public static Map<String, String> readMySqlSystemVariables(JdbcConnection connection) {
        // Read the system variables from the MySQL instance and get the current database name ...
        return querySystemVariables(connection, "SHOW VARIABLES");
    }

    private static Map<String, String> querySystemVariables(
            JdbcConnection connection, String statement) {
        final Map<String, String> variables = new HashMap<>();
        try {
            connection.query(
                    statement,
                    rs -> {
                        while (rs.next()) {
                            String varName = rs.getString(1);
                            String value = rs.getString(2);
                            if (varName != null && value != null) {
                                variables.put(varName, value);
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }

    public static BinlogOffset findBinlogOffset(
            long targetMs, MySqlConnection connection, MySqlSourceConfig mySqlSourceConfig) {
        try {
            MySqlConnection.MySqlConnectionConfiguration config = connection.connectionConfig();
            BinaryLogClient client =
                    new BinaryLogClient(
                            config.hostname(), config.port(), config.username(), config.password());
            client.setThreadFactory(
                    new ThreadFactoryBuilder()
                            .setNameFormat("timestamp-seeker-binlog-client")
                            .build());
            if (mySqlSourceConfig.getServerIdRange() != null) {
                client.setServerId(mySqlSourceConfig.getServerIdRange().getStartServerId());
            } else {
                client.setServerId(randomServerId());
            }

            LOG.info("Seeking binlog offset with timestamp {} ms", targetMs);
            try {
                // Locate the binlog file
                List<String> binlogFiles = connection.availableBinlogFiles();
                String locatedBinlogFileName = null;
                // Reverse the order to search from the latest binlog file
                binlogFiles.sort(Collections.reverseOrder());
                // Register a listener to capture the timestamp of the first event in binlog file
                FirstTimestampListener listener = new FirstTimestampListener();
                client.registerEventListener(listener);
                for (String binlogFile : binlogFiles) {
                    LOG.info("Seeker is checking binlog file: {}", binlogFile);
                    CompletableFuture<Long> timestampMsFuture = listener.resetFuture();
                    client.setBinlogFilename(binlogFile);
                    client.setBinlogPosition(0);
                    client.connect(TIMEOUT_MS);
                    long firstTimestampMs = waitForFirstTimestampMs(timestampMsFuture, binlogFile);
                    client.disconnect();
                    if (firstTimestampMs < targetMs) {
                        locatedBinlogFileName = binlogFile;
                        LOG.info(
                                "Seeker located the binlog file to \"{}\", whose starting timestamp in second is {}",
                                locatedBinlogFileName,
                                firstTimestampMs / 1000);
                        break;
                    }
                    LOG.info(
                            "The timestamp in second of the first event in binlog file \"{}\" is {}, "
                                    + "which is later than the required timestamp in second {}. Continue searching...",
                            binlogFile,
                            firstTimestampMs,
                            targetMs);
                }
                if (locatedBinlogFileName == null) {
                    LOG.info(
                            "All binlog events' timestamp are later than {}. Will use earliest offset to start.",
                            targetMs);
                    return BinlogOffset.ofEarliest();
                }

                // We directly use position 0 (the earliest position in the located binlog file) to
                // keep
                // the consistency of binlog events. It's hard to seek to the position exactly
                // matching
                // the timestamp considering metadata events like TABLE_MAP, GTID.
                return BinlogOffset.ofBinlogFilePosition(locatedBinlogFileName, 0);
            } finally {
                client.disconnect();
                client.unregisterEventListener(FirstTimestampListener.class);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Unable to seek to timestamp %d", targetMs), e);
        }
    }

    /** Copied from {@link MySqlConnectorConfig}'s randomServerId(). */
    private static int randomServerId() {
        int lowestServerId = 5400;
        int highestServerId = 6400;
        return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
    }

    private static String searchBinlogName(
            FirstTimestampListener listener,
            BinaryLogClient client,
            long targetMs,
            List<String> binlogFiles)
            throws Exception {
        int startIdx = 0;
        int endIdx = binlogFiles.size() - 1;

        while (startIdx <= endIdx) {
            int mid = startIdx + (endIdx - startIdx) / 2;
            long midTs = getBinlogTimestamp(listener, client, binlogFiles.get(mid));
            if (midTs < targetMs) {
                startIdx = mid + 1;
            } else if (targetMs < midTs) {
                endIdx = mid - 1;
            } else {
                return binlogFiles.get(mid);
            }
        }

        return endIdx < 0 ? binlogFiles.get(0) : binlogFiles.get(endIdx);
    }

    private static long getBinlogTimestamp(
            FirstTimestampListener listener, BinaryLogClient client, String binlogFile)
            throws Exception {
        try {
            LOG.info("Seeker is checking binlog file: {}", binlogFile);
            CompletableFuture<Long> timestampMsFuture = listener.resetFuture();
            client.setBinlogFilename(binlogFile);
            client.setBinlogPosition(0);
            client.connect(TIMEOUT_MS);
            return waitForFirstTimestampMs(timestampMsFuture, binlogFile);
        } finally {
            client.disconnect();
        }
    }

    private static long waitForFirstTimestampMs(CompletableFuture<Long> future, String binlogFile)
            throws Exception {
        try {
            return future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException(
                    String.format(
                            "Timeout waiting for a valid timestamp in binlog file \"%s\"",
                            binlogFile));
        }
    }

    private static class FirstTimestampListener implements BinaryLogClient.EventListener {
        private CompletableFuture<Long> timestampMsFuture = new CompletableFuture<>();

        @Override
        public void onEvent(Event event) {
            long timestampMs = event.getHeader().getTimestamp();
            if (!timestampMsFuture.isDone() && timestampMs > 0) {
                timestampMsFuture.complete(timestampMs);
            }
        }

        public CompletableFuture<Long> resetFuture() {
            timestampMsFuture = new CompletableFuture<>();
            return timestampMsFuture;
        }
    }
}
