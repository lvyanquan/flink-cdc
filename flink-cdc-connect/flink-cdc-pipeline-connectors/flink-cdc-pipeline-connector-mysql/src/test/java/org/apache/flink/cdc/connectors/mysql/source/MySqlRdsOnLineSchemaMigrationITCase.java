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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSourceFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.source.MySqlDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_PASSWORD;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.TEST_USER;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResults;
import static org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils.fetchResultsAndCreateTableEvent;

/**
 * IT case for Evolving MySQL schema with gh-ost/pt-osc utility. See <a
 * href="https://github.com/github/gh-ost">github/gh-ost</a>/<a
 * href="https://docs.percona.com/percona-toolkit/pt-online-schema-change.html">doc/pt-osc</a> for
 * more details.
 */
public class MySqlRdsOnLineSchemaMigrationITCase extends MySqlSourceTestBase {

    private static final MySqlContainer MYSQL8_CONTAINER =
            createMySqlContainer(MySqlVersion.V8_0, "docker/server-gtids/expire-seconds/my.cnf");

    private static final String DB_NAME = "testing";
    public static final String TABLE_NAME = "semver";
    public static final TableId TABLE_ID = TableId.tableId(DB_NAME, TABLE_NAME);
    private static final String USERNAME = "mysqluser";
    private static final String PASSWORD = "mysqlpw";

    protected static final Logger LOG =
            LoggerFactory.getLogger(MySqlRdsOnLineSchemaMigrationITCase.class);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @Before
    public void before() {
        Startables.deepStart(Stream.of(MYSQL8_CONTAINER)).join();
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @After
    public void after() throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL8_CONTAINER.getJdbcUrl(), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + DB_NAME);
            LOG.info("Dropped stale database {}.", DB_NAME);
        }
        MYSQL8_CONTAINER.stop();
    }

    @Test
    public void testParsingDdlEvents() throws Exception {
        List<String> expectedDdls =
                Arrays.asList(
                        "CREATE TABLE `semver` ( `id` bigint NOT NULL, `name` varchar(17) NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8",
                        "INSERT INTO semver VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Derrida'), (5, 'Eve')",
                        "CREATE TABLE `testing`.`tp_7463411_ogl_semver` ( id BIGINT auto_increment, last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `key` VARCHAR (64) charset utf8 NOT NULL, `value` VARCHAR (4096) charset utf8 NOT NULL, PRIMARY KEY (id), UNIQUE KEY key_uidx (`key`) ) auto_increment = 512",
                        "CREATE TABLE `testing`.`tp_7463411_ogt_semver` LIKE `testing`.`semver`",
                        "ALTER TABLE `testing`.`tp_7463411_ogt_semver` ADD COLUMN `gender` longtext NULL AFTER `name`",
                        "CREATE TABLE `testing`.`tp_7463411_del_semver` (`id` bigint AUTO_INCREMENT primary key)",
                        "DROP TABLE IF EXISTS `testing`.`tp_7463411_del_semver`",
                        "RENAME TABLE `testing`.`semver` to `testing`.`tp_7463411_del_semver`, `testing`.`tp_7463411_ogt_semver` to `testing`.`semver`",
                        "DROP TABLE `testing`.`tp_7463411_ogl_semver`",
                        "DROP TABLE `testing`.`tp_7463411_del_semver`",
                        "INSERT INTO semver VALUES (6, 'Ferris', 'Apache')",
                        "CREATE TABLE `testing`.`tp_7463448_ogl_semver` ( id BIGINT auto_increment, last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `key` VARCHAR (64) charset utf8 NOT NULL, `value` VARCHAR (4096) charset utf8 NOT NULL, PRIMARY KEY (id), UNIQUE KEY key_uidx (`key`) ) auto_increment = 512",
                        "CREATE TABLE `testing`.`tp_7463448_ogt_semver` LIKE `testing`.`semver`",
                        "ALTER TABLE `testing`.`tp_7463448_ogt_semver` MODIFY COLUMN `name` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL AFTER `id`",
                        "CREATE TABLE `testing`.`tp_7463448_del_semver` (`id` bigint AUTO_INCREMENT primary key)",
                        "DROP TABLE IF EXISTS `testing`.`tp_7463448_del_semver`",
                        "RENAME TABLE `testing`.`semver` to `testing`.`tp_7463448_del_semver`, `testing`.`tp_7463448_ogt_semver` to `testing`.`semver`",
                        "DROP TABLE `testing`.`tp_7463448_ogl_semver`",
                        "DROP TABLE `testing`.`tp_7463448_del_semver`",
                        "INSERT INTO semver VALUES (7, 'Gus', 'Male')",
                        "CREATE TABLE `testing`.`tp_7463486_ogl_semver` ( id BIGINT auto_increment, last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `key` VARCHAR (64) charset utf8 NOT NULL, `value` VARCHAR (4096) charset utf8 NOT NULL, PRIMARY KEY (id), UNIQUE KEY key_uidx (`key`) ) auto_increment = 512",
                        "CREATE TABLE `testing`.`tp_7463486_ogt_semver` LIKE `testing`.`semver`",
                        "ALTER TABLE `testing`.`tp_7463486_ogt_semver` CHANGE COLUMN `gender` `biological_sex` longtext CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NULL AFTER `name`",
                        "CREATE TABLE `testing`.`tp_7463486_del_semver` (`id` bigint AUTO_INCREMENT primary key)",
                        "DROP TABLE IF EXISTS `testing`.`tp_7463486_del_semver`",
                        "RENAME TABLE `testing`.`semver` to `testing`.`tp_7463486_del_semver`, `testing`.`tp_7463486_ogt_semver` to `testing`.`semver`",
                        "DROP TABLE `testing`.`tp_7463486_ogl_semver`",
                        "DROP TABLE `testing`.`tp_7463486_del_semver`",
                        "INSERT INTO semver VALUES (8, 'Helen', 'Female')",
                        "CREATE TABLE `testing`.`tp_7463522_ogl_semver` ( id BIGINT auto_increment, last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `key` VARCHAR (64) charset utf8 NOT NULL, `value` VARCHAR (4096) charset utf8 NOT NULL, PRIMARY KEY (id), UNIQUE KEY key_uidx (`key`) ) auto_increment = 512",
                        "CREATE TABLE `testing`.`tp_7463522_ogt_semver` LIKE `testing`.`semver`",
                        "ALTER TABLE `testing`.`tp_7463522_ogt_semver` DROP COLUMN `biological_sex`",
                        "CREATE TABLE `testing`.`tp_7463522_del_semver` (`id` bigint AUTO_INCREMENT primary key)",
                        "DROP TABLE IF EXISTS `testing`.`tp_7463522_del_semver`",
                        "RENAME TABLE `testing`.`semver` to `testing`.`tp_7463522_del_semver`, `testing`.`tp_7463522_ogt_semver` to `testing`.`semver`",
                        "DROP TABLE `testing`.`tp_7463522_ogl_semver`",
                        "DROP TABLE `testing`.`tp_7463522_del_semver`",
                        "INSERT INTO semver VALUES (9, 'IINA')");
        List<String> actualDdls =
                readBinlogDdlEvents("rds-online-schema-change-binlog", TABLE_NAME);
        Assert.assertEquals(expectedDdls, actualDdls);
    }

    @Test
    public void testParsingMySqlOnLineSchemaChangeEvents() throws Exception {
        LOG.info("Step 1: Load & replicate DDL changes...");
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL8_CONTAINER.getJdbcUrl(), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + DB_NAME);
        }

        List<String> ddlEvents = readBinlogDdlEvents("rds-online-schema-change-binlog", TABLE_NAME);

        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL8_CONTAINER.getJdbcUrl(DB_NAME), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            // Create table first to avoid binlog schema out-of-sync
            statement.execute(ddlEvents.get(0));

            // Insert some snapshot data
            statement.execute(ddlEvents.get(1));
        }

        LOG.info("Step 2: Starting pipeline job...");
        MySqlSourceConfigFactory configFactory =
                new MySqlSourceConfigFactory()
                        .hostname(MYSQL8_CONTAINER.getHost())
                        .port(MYSQL8_CONTAINER.getDatabasePort())
                        .username(TEST_USER)
                        .password(TEST_PASSWORD)
                        .databaseList(DB_NAME)
                        .tableList(DB_NAME + "." + TABLE_NAME)
                        .startupOptions(StartupOptions.earliest())
                        .serverId(getServerId(env.getParallelism()))
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(SCHEMA_CHANGE_ENABLED.defaultValue())
                        .parseOnLineSchemaChanges(true);

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MySqlDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MySqlDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);

        List<Event> expected = new ArrayList<>();

        LOG.info("Step 3: Verifying CreateTableEvent & snapshot data...");
        // CreateTableEvent
        {
            Schema schemaV1 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(17))
                            .primaryKey(Collections.singletonList("id"))
                            .build();
            expected.add(generateInsert(schemaV1, 1L, "Alice"));
            expected.add(generateInsert(schemaV1, 2L, "Bob"));
            expected.add(generateInsert(schemaV1, 3L, "Carol"));
            expected.add(generateInsert(schemaV1, 4L, "Derrida"));
            expected.add(generateInsert(schemaV1, 5L, "Eve"));

            Tuple2<List<Event>, List<CreateTableEvent>> actual =
                    fetchResultsAndCreateTableEvent(events, expected.size());

            Assertions.assertThat(actual.f0).containsExactlyInAnyOrderElementsOf(expected);
            Assertions.assertThat(actual.f1).containsOnly(new CreateTableEvent(TABLE_ID, schemaV1));
        }

        expected.clear();

        LOG.info("Step 4: Starting schema evolution...");
        try (Connection connection =
                        DriverManager.getConnection(
                                MYSQL8_CONTAINER.getJdbcUrl(DB_NAME), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            ddlEvents.stream()
                    .skip(2)
                    .forEach(
                            sql -> {
                                try {
                                    LOG.info("Executing {}...", sql);
                                    statement.execute(sql);
                                } catch (SQLException e) {
                                    LOG.error("Failed to execute {}...", sql, e);
                                    throw new RuntimeException(e);
                                }
                            });
        }

        // AddColumnEvent
        {
            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(17))
                            .physicalColumn("gender", DataTypes.STRING())
                            .primaryKey(Collections.singletonList("id"))
                            .build();
            expected.add(
                    new AddColumnEvent(
                            TABLE_ID,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            new PhysicalColumn("gender", DataTypes.STRING(), null),
                                            AddColumnEvent.ColumnPosition.AFTER,
                                            "name"))));
            expected.add(generateInsert(schemaV2, 6L, "Ferris", "Apache"));
        }

        // AlterColumnTypeEvent
        {
            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(34))
                            .physicalColumn("gender", DataTypes.STRING())
                            .primaryKey(Collections.singletonList("id"))
                            .build();
            expected.add(
                    new AlterColumnTypeEvent(
                            TABLE_ID, Collections.singletonMap("name", DataTypes.VARCHAR(34))));
            expected.add(generateInsert(schemaV3, 7L, "Gus", "Male"));
        }

        // RenameColumnEvent & AlterColumnTypeEvent
        {
            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(34))
                            .physicalColumn("biological_sex", DataTypes.STRING())
                            .primaryKey(Collections.singletonList("id"))
                            .build();
            expected.add(
                    new AlterColumnTypeEvent(
                            TABLE_ID, Collections.singletonMap("gender", DataTypes.STRING())));
            expected.add(
                    new RenameColumnEvent(
                            TABLE_ID, Collections.singletonMap("gender", "biological_sex")));
            expected.add(generateInsert(schemaV4, 8L, "Helen", "Female"));
        }

        // DropColumnEvent
        {
            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(34))
                            .primaryKey(Collections.singletonList("id"))
                            .build();
            expected.add(
                    new DropColumnEvent(TABLE_ID, Collections.singletonList("biological_sex")));
            expected.add(generateInsert(schemaV5, 9L, "IINA"));
        }

        {
            List<Event> actual = fetchResults(events, expected.size());
            assertEqualsInAnyOrder(
                    expected.stream().map(Object::toString).collect(Collectors.toList()),
                    actual.stream().map(Object::toString).collect(Collectors.toList()));
        }
    }

    private static List<String> readBinlogDdlEvents(String binlogFileName, String targetTable)
            throws URISyntaxException, IOException {
        LOG.info("Parsing Binlog file {}...", binlogFileName);
        List<com.github.shyiko.mysql.binlog.event.Event> events = new ArrayList<>();
        try (BinaryLogFileReader reader =
                new BinaryLogFileReader(
                        Paths.get(
                                        Objects.requireNonNull(
                                                        MySqlRdsOnLineSchemaMigrationITCase.class
                                                                .getClassLoader()
                                                                .getResource(
                                                                        "binlog/" + binlogFileName))
                                                .toURI())
                                .toFile())) {
            com.github.shyiko.mysql.binlog.event.Event event = reader.readEvent();
            while (event != null) {
                events.add(event);
                event = reader.readEvent();
            }
        }

        LOG.info("Successfully parsed Binlog file {}...", binlogFileName);

        return events.stream()
                .filter(
                        evt ->
                                EventType.QUERY.equals(evt.getHeader().getEventType())
                                        || EventType.ROWS_QUERY.equals(
                                                evt.getHeader().getEventType()))
                .map(com.github.shyiko.mysql.binlog.event.Event::getData)
                .flatMap(data -> extractDdlEvents(targetTable, data))
                .map(MySqlRdsOnLineSchemaMigrationITCase::sanitizeDdl)
                .collect(Collectors.toList());
    }

    @NotNull
    private static Stream<String> extractDdlEvents(String targetTable, Object data) {
        if (data instanceof QueryEventData) {
            QueryEventData queryEventData = (QueryEventData) data;
            String sql = queryEventData.getSql();
            if (!sql.equals("BEGIN") && sql.contains(targetTable)) {
                return Stream.of(sql);
            }
        } else if (data instanceof RowsQueryEventData) {
            RowsQueryEventData rowsQueryEventData = (RowsQueryEventData) data;
            String sql = rowsQueryEventData.getQuery();
            if (sql.contains(targetTable)) {
                return Stream.of(rowsQueryEventData.getQuery());
            }
        }
        return Stream.empty();
    }

    private static String sanitizeDdl(String ddl) {
        while (ddl.contains("/*") && ddl.contains("*/")) {
            String prefix = ddl.substring(0, ddl.indexOf("/*"));
            String suffix = ddl.substring(ddl.indexOf("*/") + 2);
            ddl = prefix + suffix;
        }
        return ddl.replace("\n", " ").replace("\t", "").trim();
    }

    private DataChangeEvent generateInsert(Schema schema, Object... fields) {
        return DataChangeEvent.insertEvent(TABLE_ID, generate(schema, fields));
    }

    private BinaryRecordData generate(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }
}
