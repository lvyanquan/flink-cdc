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

/** Test cases for {@link Migrator} with {@code MigratableConnector.UPSERT_KAFKA}. */
class UpsertKafkaConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "upsert-kafka")
                                        .put(
                                                "properties.bootstrap.servers",
                                                "127.0.0.1:9980,10.0.0.4:10000")
                                        .put("key.format", "debezium-json")
                                        .put("value.format", "debezium-json")
                                        .put("value.fields-include", "ALL")
                                        .put("topic", "big_topic")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "upsert-kafka")
                                .put(
                                        "properties.bootstrap.servers",
                                        "127.0.0.1:9980,10.0.0.4:10000")
                                .put("topic", "big_topic")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "upsert-kafka")
                                        .put(
                                                "properties.bootstrap.servers",
                                                "127.0.0.1:9980,10.0.0.4:10000")
                                        .put("properties.allow.auto.create.topics", "false")
                                        .put("key.format", "debezium-json")
                                        .put("key.fields-prefix", "prefix_")
                                        .put("value.format", "debezium-json")
                                        .put("value.fields-include", "ALL")
                                        .put("topic", "big_topic")
                                        .put("sink.parallelism", "4")
                                        .put("sink.buffer-flush.max-rows", "4097")
                                        .put("sink.buffer-flush.interval", "3s")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "upsert-kafka")
                                .put(
                                        "properties.bootstrap.servers",
                                        "127.0.0.1:9980,10.0.0.4:10000")
                                .put("properties.allow.auto.create.topics", "false")
                                .put("topic", "big_topic")
                                .build());
    }

    @Test
    void testMigratingUpsertKafkaSource() {
        Assertions.assertThatThrownBy(
                        () ->
                                Assertions.assertThat(
                                        Migrator.migrateSource(
                                                buildMap().put("type", "upsert-kafka").build())))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`upsert-kafka` could not be used as a YAML source.");
    }
}
