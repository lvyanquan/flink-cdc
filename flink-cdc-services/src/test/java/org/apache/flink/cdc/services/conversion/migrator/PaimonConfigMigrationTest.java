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

/** Test cases for {@link Migrator} with {@code MigratableConnector.PAIMON}. */
class PaimonConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "paimon")
                                        .put("path", "/opt/paimon")
                                        .put("metastore", "filesystem")
                                        .put("warehouse", "/opt/paimon/")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "paimon")
                                .put("catalog.properties.metastore", "filesystem")
                                .put("catalog.properties.warehouse", "/opt/paimon/")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "paimon")
                                        .put("path", "/root/paimon")
                                        .put("write-mode", "append-only")
                                        .put("sink.parallelism", "4")
                                        .put("partial-update.ignore-delete", "true")
                                        .put("auto-create", "true")
                                        .put("bucket", "17")
                                        .put("bucket-key", "order_id,cust_id")
                                        .put("full-compaction.delta-commits", "24")
                                        .put("merge-engine", "deduplicate")
                                        .put("partition.default-name", "__DEFAULT_PARTITION__")
                                        .put("partition.expiration-check-interval", "2h")
                                        .put("partition.expiration-time", "5min")
                                        .put("partition.timestamp-formatter", "yyyy-MM-dd")
                                        .put("partition.timestamp-pattern", "yyyy-MM-dd")
                                        .put("snapshot.num-retained.max", "35")
                                        .put("snapshot.num-retained.min", "10")
                                        .put("metastore", "dlf")
                                        .put("warehouse", "oss://<bucket>/<object>")
                                        .put("fs.oss.endpoint", "oss-cn-nanjing.aliyuncs.com")
                                        .put("fs.oss.accessKeyId", "__ak_id__")
                                        .put("fs.oss.accessKeySecret", "__ak_secret__")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "paimon")
                                .put("table.properties.partial-update.ignore-delete", "true")
                                .put("table.properties.auto-create", "true")
                                .put("table.properties.bucket", "17")
                                .put("table.properties.bucket-key", "order_id,cust_id")
                                .put("table.properties.full-compaction.delta-commits", "24")
                                .put("table.properties.merge-engine", "deduplicate")
                                .put(
                                        "table.properties.partition.default-name",
                                        "__DEFAULT_PARTITION__")
                                .put("table.properties.partition.expiration-check-interval", "2h")
                                .put("table.properties.partition.expiration-time", "5min")
                                .put("table.properties.partition.timestamp-formatter", "yyyy-MM-dd")
                                .put("table.properties.partition.timestamp-pattern", "yyyy-MM-dd")
                                .put("table.properties.snapshot.num-retained.max", "35")
                                .put("table.properties.snapshot.num-retained.min", "10")
                                .put("catalog.properties.metastore", "dlf")
                                .put("catalog.properties.warehouse", "oss://<bucket>/<object>")
                                .put(
                                        "catalog.properties.fs.oss.endpoint",
                                        "oss-cn-nanjing.aliyuncs.com")
                                .put("catalog.properties.fs.oss.accessKeyId", "__ak_id__")
                                .put("catalog.properties.fs.oss.accessKeySecret", "__ak_secret__")
                                .build());
    }

    @Test
    void testMigratingPaimonSource() {
        Assertions.assertThatThrownBy(
                        () ->
                                Assertions.assertThat(
                                        Migrator.migrateSource(
                                                buildMap().put("type", "paimon").build())))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`paimon` could not be used as a YAML source.");
    }
}
