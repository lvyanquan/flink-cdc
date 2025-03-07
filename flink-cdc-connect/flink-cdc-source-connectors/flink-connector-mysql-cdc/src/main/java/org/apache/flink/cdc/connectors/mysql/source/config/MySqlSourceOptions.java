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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link MySqlSource}. */
public class MySqlSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the MySQL database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Integer port number of the MySQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("userName")
                    .withDescription(
                            "Name of the MySQL database to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the MySQL database server.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the MySQL server to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("tableName")
                    .withDescription("Table name of the MySQL database to monitor.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<String> SERVER_ID =
            ConfigOptions.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408', The numeric ID range syntax is recommended "
                                    + ". Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withFallbackKeys("connectionMaxWait")
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the MySQL database server before timing out. This value cannot be less than 250ms.");

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            ConfigOptions.key("connection.pool.size")
                    .intType()
                    .defaultValue(20)
                    .withFallbackKeys("connectionMaxActive")
                    .withDescription("The connection pool size.");

    public static final ConfigOption<Integer> CONNECT_MAX_RETRIES =
            ConfigOptions.key("connect.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times that the connector should retry to build MySQL database server connection.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for MySQL CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_FILE =
            ConfigOptions.key("scan.startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file name used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_POS =
            ConfigOptions.key("scan.startup.specific-offset.pos")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file position used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET =
            ConfigOptions.key("scan.startup.specific-offset.gtid-set")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional GTID set used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS =
            ConfigOptions.key("scan.startup.specific-offset.skip-events")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional number of events to skip after the specific starting offset");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS =
            ConfigOptions.key("scan.startup.specific-offset.skip-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional number of rows to skip after the specific offset");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final String INTERNAL_PREFIX = "__internal";

    public static final ConfigOption<Boolean> INTERNAL_IS_SHARDING_TABLE =
            ConfigOptions.key(String.format("%s.is-sharding-table", INTERNAL_PREFIX))
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The specified flink table is a sharding table in the external database. The option also determines whether add system columns for the schema.");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available binlog offsets");

    /** See https://aliyuque.antfin.com/ververica/connectors/rge6g0 for more details. */
    public static final ConfigOption<Long> VERVERICA_START_TIME_MILLS =
            ConfigOptions.key("startTimeMs")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "Timestamp in millisecond specifying the time when the connector starts consuming from. "
                                    + "This option is only used for specifying startup timestamp by VVP UI and cannot "
                                    + "be used in CREATE TABLE WITH options.");

    // ----------------------------------------------------------------------------
    // experimental options, won't add them to documentation
    // ----------------------------------------------------------------------------
    @Experimental
    public static final ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query MySQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query MySQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            ConfigOptions.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether capture the scan the newly added tables or not, by default is false. This option is only useful when we start the job from a savepoint/checkpoint.");

    @Experimental
    public static final ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table."
                                    + "By default, the chunk key is the first column of the primary key.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase. This feature depends on "
                                    + "FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be "
                                    + "greater than or equal to 1.14 when enabling this feature.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP =
            ConfigOptions.key("scan.incremental.snapshot.backfill.skip")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to skip backfill in snapshot reading phase. If backfill is skipped, changes on captured tables during snapshot phase will be consumed later in binlog reading phase instead of being merged into the snapshot. WARNING: Skipping backfill might lead to data inconsistency because some binlog events happened within the snapshot phase might be replayed (only at-least-once semantic is promised). For example updating an already updated value in snapshot, or deleting an already deleted entry in snapshot. These replayed binlog events should be handled specially.");

    @Experimental
    public static final ConfigOption<Boolean> PARSE_ONLINE_SCHEMA_CHANGES =
            ConfigOptions.key("scan.parse.online.schema.changes.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to parse schema change events generated by gh-ost/pt-osc utilities. Defaults to false.");

    @Experimental
    public static final ConfigOption<Boolean> USE_LEGACY_JSON_FORMAT =
            ConfigOptions.key("use.legacy.json.format")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use legacy json format. The default value is true, which means there is no whitespace before value and after comma in json format.");

    @Experimental
    public static final ConfigOption<Boolean>
            SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED =
                    ConfigOptions.key("scan.only.deserialize.captured.tables.changelog.enabled")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "Whether to deserialize only changelog Events of user defined captured tables, thus we can speed up the binlog process procedure.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED =
            ConfigOptions.key("scan.read-changelog-as-append-only.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to convert all changelogs as append only during reading Binlog data. If set to true, changelog will be read as +I (insert), -U (update before), +U (update after), and -D (delete). Otherwise, all changelog will be treated as +I (insert).");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_PARALLEL_DESERIALIZE_CHANGELOG_ENABLED =
            ConfigOptions.key("scan.parallel-deserialize-changelog.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to deserialize only changelog Events of user defined captured tables, thus we can speed up the binlog process procedure.");

    @Experimental
    public static final ConfigOption<Integer> SCAN_PARALLEL_DESERIALIZE_CHANGELOG_HANDLER_SIZE =
            ConfigOptions.key("scan.parallel-deserialize-changelog.handler.size")
                    .intType()
                    .defaultValue(2)
                    .withDescription("The size of the event handler during parallel conversion.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST =
            ConfigOptions.key("scan.incremental.snapshot.unbounded-chunk-first.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to assign the unbounded chunks first during snapshot reading phase. This might help reduce the risk of the TaskManager experiencing an out-of-memory (OOM) error when taking a snapshot of the largest unbounded chunk. Defaults to false.");
}
