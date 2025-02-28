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

package org.apache.flink.cdc.services.conversion.migrator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.services.utils.ServiceTestUtils.buildMap;

/** Test cases for {@link Migrator} with {@code MigratableConnector.MYSQL}. */
class MySQLConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "mysql")
                                        .put("hostname", "localhost")
                                        .put("username", "root")
                                        .put("password", "12345678")
                                        .put("database-name", "dummy-db")
                                        .put("table-name", "dummy-tbl")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "mysql")
                                .put("hostname", "localhost")
                                .put("username", "root")
                                .put("password", "12345678")
                                .build());
    }

    @Test
    void testMigratingMinimalConfigurationsWithAlias() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "mysql-cdc")
                                        .put("hostname", "localhost")
                                        .put("username", "root")
                                        .put("password", "12345678")
                                        .put("database-name", "dummy-db")
                                        .put("table-name", "dummy-tbl")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "mysql")
                                .put("hostname", "localhost")
                                .put("username", "root")
                                .put("password", "12345678")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "mysql")
                                        .put("hostname", "localhost")
                                        .put("username", "root")
                                        .put("password", "12345678")
                                        .put("database-name", "dummy-db")
                                        .put("table-name", "dummy-tbl")
                                        .put("server-id", "5400-5404")
                                        .put("scan.incremental.snapshot.enabled", "true")
                                        .put("scan.incremental.snapshot.chunk.size", "8097")
                                        .put("scan.snapshot.fetch.size", "1025")
                                        .put("scan.startup.mode", "initial")
                                        .put(
                                                "scan.startup.specific-offset.file",
                                                "mysql-bin.000003")
                                        .put("scan.startup.specific-offset.pos", "42")
                                        .put(
                                                "scan.startup.specific-offset.gtid-set",
                                                "24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")
                                        .put("scan.startup.timestamp-millis", "896743672100")
                                        .put("server-time-zone", "Asia/Shanghai")
                                        .put("debezium.min.row.count.to.stream.results", "1001")
                                        .put("connect.timeout", "31s")
                                        .put("connect.max-retries", "4")
                                        .put("connection.pool.size", "21")
                                        .put("jdbc.properties.useSSL", "false")
                                        .put(
                                                "debezium.event.deserialization.failure.handling.mode",
                                                "ignore")
                                        .put("heartbeat.interval", "31s")
                                        .put("scan.incremental.snapshot.chunk.key-column", "id")
                                        .put("rds.region-id", "cn-shanghai")
                                        .put("rds.access-key-id", "__access_key_id__")
                                        .put("rds.access-key-secret", "__access_key_secret__")
                                        .put("rds.db-instance-id", "8y78fuh4382pa0")
                                        .put("rds.main-db-id", "main_db")
                                        .put("rds.download.timeout", "61s")
                                        .put("rds.endpoint", "rds.aliyuncs.com")
                                        .put("scan.incremental.close-idle-reader.enabled", "true")
                                        .put("scan.read-changelog-as-append-only.enabled", "true")
                                        .put(
                                                "scan.only.deserialize.captured.tables.changelog.enabled",
                                                "true")
                                        .put("scan.parallel-deserialize-changelog.enabled", "true")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "mysql")
                                .put("hostname", "localhost")
                                .put("username", "root")
                                .put("password", "12345678")
                                .put("server-id", "5400-5404")
                                .put("scan.incremental.snapshot.enabled", "true")
                                .put("scan.incremental.snapshot.chunk.size", "8097")
                                .put("scan.snapshot.fetch.size", "1025")
                                .put("scan.startup.mode", "initial")
                                .put("scan.startup.specific-offset.file", "mysql-bin.000003")
                                .put("scan.startup.specific-offset.pos", "42")
                                .put(
                                        "scan.startup.specific-offset.gtid-set",
                                        "24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")
                                .put("scan.startup.timestamp-millis", "896743672100")
                                .put("server-time-zone", "Asia/Shanghai")
                                .put("debezium.min.row.count.to.stream.results", "1001")
                                .put("connect.timeout", "31s")
                                .put("connect.max-retries", "4")
                                .put("connection.pool.size", "21")
                                .put("jdbc.properties.useSSL", "false")
                                .put(
                                        "debezium.event.deserialization.failure.handling.mode",
                                        "ignore")
                                .put("heartbeat.interval", "31s")
                                .put("scan.incremental.snapshot.chunk.key-column", "id")
                                .put("rds.region-id", "cn-shanghai")
                                .put("rds.access-key-id", "__access_key_id__")
                                .put("rds.access-key-secret", "__access_key_secret__")
                                .put("rds.db-instance-id", "8y78fuh4382pa0")
                                .put("rds.main-db-id", "main_db")
                                .put("rds.download.timeout", "61s")
                                .put("rds.endpoint", "rds.aliyuncs.com")
                                .put("scan.incremental.close-idle-reader.enabled", "true")
                                .put(
                                        "scan.only.deserialize.captured.tables.changelog.enabled",
                                        "true")
                                .put("scan.parallel-deserialize-changelog.enabled", "true")
                                .build());
    }

    @Test
    void testMigratingMySqlSink() {
        Assertions.assertThatThrownBy(
                        () ->
                                Migrator.migrateSink(
                                        buildMap()
                                                .put("type", "mysql")
                                                .put("hostname", "localhost")
                                                .put("username", "root")
                                                .put("password", "12345678")
                                                .put("database-name", "dummy-db")
                                                .put("table-name", "dummy-tbl")
                                                .build()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`mysql` could not be used as a YAML sink.");
    }
}
