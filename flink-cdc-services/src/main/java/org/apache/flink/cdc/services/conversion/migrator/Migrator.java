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

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/** Migrates CTAS or CDAS connector configurations to CDC YAML equivalents. */
public class Migrator {

    private static final int sourceTypeTag = 1;
    private static final int sinkTypeTag = 2;

    private Migrator() {}

    private static final Map<String, MigratableConnector> CONNECTOR_MAP =
            ImmutableMap.of(
                    "mysql", MigratableConnector.MYSQL,
                    "kafka", MigratableConnector.KAFKA,
                    "upsert-kafka", MigratableConnector.UPSERT_KAFKA,
                    "starrocks", MigratableConnector.STARROCKS,
                    "hologres", MigratableConnector.HOLOGRES,
                    "paimon", MigratableConnector.PAIMON);

    /** Tries to migrate CXAS source connector options to CDC YAML. */
    public static Map<String, String> migrateSource(final Map<String, String> originalConfig) {
        return migrate(sourceTypeTag, originalConfig);
    }

    /** Tries to migrate CXAS sink connector options to CDC YAML. */
    public static Map<String, String> migrateSink(final Map<String, String> originalConfig) {
        return migrate(sinkTypeTag, originalConfig);
    }

    private static Map<String, String> migrate(int type, final Map<String, String> originalConfig) {
        String connectorName = originalConfig.get("type");

        // In CTAS mode, `mysql-cdc` is an acceptable synonym of `mysql`.
        if ("mysql-cdc".equals(connectorName)) {
            connectorName = "mysql";
        }

        MigratableConnector connector = CONNECTOR_MAP.get(connectorName);

        // This is for testing purposes only, avoid adding it into CONNECTOR_MAP
        if ("mock-test-filesystem".equals(connectorName)) {
            connector = MigratableConnector.MOCK;
        }

        if (connector == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "CDC YAML does not support migrating jobs with `%s` connector. "
                                    + "Currently, only these connectors are supported: %s",
                            connectorName, String.join(", ", CONNECTOR_MAP.keySet())));
        }

        if (type == sourceTypeTag) {
            Preconditions.checkArgument(
                    connector.isSource, "`%s` could not be used as a YAML source.", connectorName);
        } else if (type == sinkTypeTag) {
            Preconditions.checkArgument(
                    connector.isSink, "`%s` could not be used as a YAML sink.", connectorName);
        } else {
            throw new IllegalArgumentException("Unexpected connector type tag: " + type);
        }

        Map<String, String> migratedConfig = new HashMap<>();

        // Always put `type` config first
        migratedConfig.put("type", connectorName);

        // Migrate existing options, for best effort
        for (MigrationRule rule : connector.rules) {
            rule.accept(originalConfig, migratedConfig);
        }

        return migratedConfig;
    }
}
