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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.entry;

/** Test cases for {@link MigrationRule}. */
class MigrationRuleTest {

    private static final Map<String, String> CONFIG_MAP =
            ImmutableMap.of(
                    "key1", "value1",
                    "key2", "value2",
                    "key3.1", "value3.1",
                    "key3.2", "value3.2",
                    "key3.3", "value3.3");

    @Test
    void testSingleAsIsMigration() {
        Assertions.assertThat(testWithRules(CONFIG_MAP, MigrationRule.asIs("key1")))
                .containsExactly(entry("key1", "value1"));
    }

    @Test
    void testMultipleAsIsMigration() {
        Assertions.assertThat(testWithRules(CONFIG_MAP, MigrationRule.asIs("key1", "key2")))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of("key1", "value1", "key2", "value2"));
    }

    @Test
    void testAsIsWithPredicateMigration() {
        Assertions.assertThat(
                        testWithRules(
                                CONFIG_MAP, MigrationRule.asIs(key -> key.startsWith("key3"))))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                "key3.1", "value3.1",
                                "key3.2", "value3.2",
                                "key3.3", "value3.3"));
    }

    @Test
    void testSingleRenameMigration() {
        Assertions.assertThat(testWithRules(CONFIG_MAP, MigrationRule.rename("key1", "key1-neo")))
                .containsExactly(entry("key1-neo", "value1"));
    }

    @Test
    void testRenameWithPredicateMigration() {
        Assertions.assertThat(
                        testWithRules(
                                CONFIG_MAP,
                                MigrationRule.rename(
                                        (key) -> key.startsWith("key3"),
                                        (key) -> key.replace("key3", "key3-neo"))))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                "key3-neo.1", "value3.1",
                                "key3-neo.2", "value3.2",
                                "key3-neo.3", "value3.3"));
    }

    @Test
    void testSingleDropMigration() {
        Assertions.assertThat(
                        testWithRules(
                                CONFIG_MAP,
                                MigrationRule.asIs((key) -> true),
                                MigrationRule.drop("key3.1")))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                "key1", "value1",
                                "key2", "value2",
                                "key3.2", "value3.2",
                                "key3.3", "value3.3"));
    }

    @Test
    void testMultipleDropMigration() {
        Assertions.assertThat(
                        testWithRules(
                                CONFIG_MAP,
                                MigrationRule.asIs((key) -> true),
                                MigrationRule.drop("key3.1", "key3.3")))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                "key1", "value1",
                                "key2", "value2",
                                "key3.2", "value3.2"));
    }

    private Map<String, String> testWithRules(
            Map<String, String> original, MigrationRule... rules) {
        Map<String, String> result = new HashMap<>();
        for (MigrationRule rule : rules) {
            rule.accept(original, result);
        }
        return result;
    }
}
