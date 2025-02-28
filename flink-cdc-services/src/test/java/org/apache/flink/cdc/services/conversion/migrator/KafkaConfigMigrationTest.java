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

/** Test cases for {@link Migrator} with {@code MigratableConnector.KAFKA}. */
class KafkaConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "kafka")
                                        .put(
                                                "properties.bootstrap.servers",
                                                "127.0.0.1:9980,10.0.0.4:10000")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "kafka")
                                .put(
                                        "properties.bootstrap.servers",
                                        "127.0.0.1:9980,10.0.0.4:10000")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "kafka")
                                        .put(
                                                "properties.bootstrap.servers",
                                                "127.0.0.1:9980,10.0.0.4:10000")
                                        .put("properties.allow.auto.create.topics", "false")
                                        .put("format", "json")
                                        .put("key.format", "json")
                                        .put("key.fields", "field1;field2")
                                        .put("key.fields-prefix", "prefix_")
                                        .put("value.format", "json")
                                        .put("value.fields-include", "ALL")
                                        .put("topic", "big_topic")
                                        .put("topic-pattern", "topic_.*")
                                        .put("properties.group.id", "KafkaSource-tbl")
                                        .put("scan.startup.mode", "earliest-offset")
                                        .put(
                                                "scan.startup.specific-offsets",
                                                "partition:0,offset:42;partition:1,offset:300")
                                        .put("scan.startup.timestamp-millis", "9874832948000")
                                        .put("scan.topic-partition-discovery.interval", "6min")
                                        .put(
                                                "scan.header-filter",
                                                "depart:toy|depart:book&!env:test")
                                        .put("scan.check.duplicated.group.id", "false")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "kafka")
                                .put(
                                        "properties.bootstrap.servers",
                                        "127.0.0.1:9980,10.0.0.4:10000")
                                .put("properties.allow.auto.create.topics", "false")
                                .put("value.format", "json")
                                .put("topic", "big_topic")
                                .put("topic-pattern", "topic_.*")
                                .put("properties.group.id", "KafkaSource-tbl")
                                .put("scan.startup.mode", "earliest-offset")
                                .put(
                                        "scan.startup.specific-offsets",
                                        "partition:0,offset:42;partition:1,offset:300")
                                .put("scan.startup.timestamp-millis", "9874832948000")
                                .put("scan.topic-partition-discovery.interval", "6min")
                                .put("scan.check.duplicated.group.id", "false")
                                .build());
    }

    @Test
    void testMigratingFormatConfig() {
        Assertions.assertThat(
                        Migrator.migrateSource(
                                buildMap()
                                        .put("type", "kafka")
                                        .put(
                                                "properties.bootstrap.servers",
                                                "127.0.0.1:9980,10.0.0.4:10000")
                                        .put("format", "json")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "kafka")
                                .put(
                                        "properties.bootstrap.servers",
                                        "127.0.0.1:9980,10.0.0.4:10000")
                                .put("value.format", "json")
                                .build());
    }

    @Test
    void testMigratingKafkaSink() {
        Assertions.assertThatThrownBy(
                        () ->
                                Assertions.assertThat(
                                        Migrator.migrateSink(
                                                buildMap()
                                                        .put("type", "kafka")
                                                        .put(
                                                                "properties.bootstrap.servers",
                                                                "127.0.0.1:9980,10.0.0.4:10000")
                                                        .build())))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`kafka` could not be used as a YAML sink.");
    }
}
