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

/** Test cases for {@link Migrator} with {@code MigratableConnector.STARROCKS}. */
class StarRocksConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "starrocks")
                                        .put("jdbc-url", "jdbc:mysql://localhost:9030")
                                        .put("database-name", "db")
                                        .put("table-name", "tbl")
                                        .put("username", "root")
                                        .put("password", "12345678")
                                        .put("load-url", "localhost:8030;localhost:8031")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "starrocks")
                                .put("jdbc-url", "jdbc:mysql://localhost:9030")
                                .put("username", "root")
                                .put("password", "12345678")
                                .put("load-url", "localhost:8030;localhost:8031")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "starrocks")
                                        .put("jdbc-url", "jdbc:mysql://localhost:9030")
                                        .put("database-name", "db")
                                        .put("table-name", "tbl")
                                        .put("username", "root")
                                        .put("password", "12345678")
                                        .put("starrocks.create.table.properties.buckets", "8")
                                        .put(
                                                "starrocks.create.table.properties.replication_num",
                                                "1")
                                        .put("load-url", "localhost:8030;localhost:8031")
                                        .put("sink.semantic", "at-least-once")
                                        .put("sink.buffer-flush.max-bytes", "65mb")
                                        .put("sink.buffer-flush.max-rows", "600000")
                                        .put("sink.buffer-flush.interval-ms", "400000")
                                        .put("sink.max-retries", "4")
                                        .put("sink.connect.timeout-ms", "2000")
                                        .put("sink.properties.format", "csv")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "starrocks")
                                .put("jdbc-url", "jdbc:mysql://localhost:9030")
                                .put("username", "root")
                                .put("password", "12345678")
                                .put("table.create.properties.buckets", "8")
                                .put("table.create.properties.replication_num", "1")
                                .put("load-url", "localhost:8030;localhost:8031")
                                .put("sink.semantic", "at-least-once")
                                .put("sink.buffer-flush.max-bytes", "65mb")
                                .put("sink.buffer-flush.max-rows", "600000")
                                .put("sink.buffer-flush.interval-ms", "400000")
                                .put("sink.max-retries", "4")
                                .put("sink.connect.timeout-ms", "2000")
                                .put("sink.properties.format", "csv")
                                .build());
    }

    @Test
    void testMigratingStarRocksSource() {
        Assertions.assertThatThrownBy(
                        () ->
                                Assertions.assertThat(
                                        Migrator.migrateSource(
                                                buildMap().put("type", "starrocks").build())))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`starrocks` could not be used as a YAML source.");
    }
}
