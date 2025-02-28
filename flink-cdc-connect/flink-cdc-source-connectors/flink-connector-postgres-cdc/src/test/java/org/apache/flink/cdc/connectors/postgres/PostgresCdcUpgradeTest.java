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

package org.apache.flink.cdc.connectors.postgres;

import org.apache.flink.cdc.connectors.postgres.table.PostgreSQLTableFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.OptionSnapshot;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.planner.runtime.stream.sql.OptionUpgradeTestBase;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Test for postgres compatibility. */
public class PostgresCdcUpgradeTest extends OptionUpgradeTestBase {
    @Override
    protected Map<String, String> getEnrichmentOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "postgres-cdc");
        options.put("hostname", "localhost");
        options.put("port", "3307");
        options.put("username", "test-user-1");
        options.put("password", "test-password-1");
        options.put("database-name", "test-db");
        options.put("schema-name", "test-schema");
        options.put("scan.incremental.snapshot.chunk.size", "100");
        options.put("scan.startup.mode", "initial");
        options.put("slot.name", "test-slot");
        options.put("table-name", "test-tbl");
        return options;
    }

    @Override
    protected ObjectIdentifier createSourceTable() throws Exception {
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE customers ("
                        + " id BIGINT NOT NULL,"
                        + " name STRING,"
                        + " address STRING,"
                        + " phone_number STRING,"
                        + " primary key (id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'postgres-cdc',"
                        + " 'hostname' = 'localhost',"
                        + " 'port' = '3306',"
                        + " 'username' = 'test-user',"
                        + " 'password' = 'test-password',"
                        + " 'database-name' = 'test-db',"
                        + " 'schema-name' = 'test-schema',"
                        + " 'table-name' = 'test-tbl',"
                        + " 'slot.name' = 'test-slot',"
                        + " 'scan.startup.mode' = 'initial',"
                        + " 'scan.incremental.snapshot.chunk.size' = '100'"
                        + ")");
        return ObjectIdentifier.of(
                tableEnv.getCurrentCatalog(), tableEnv.getCurrentDatabase(), "customers");
    }

    @Override
    protected DynamicTableSourceFactory getSourceFactory() {
        return new PostgreSQLTableFactory();
    }

    @Override
    protected OptionSnapshot getExpectedSourceSnapshot(ObjectIdentifier objectIdentifier) {
        Map<String, String> options = new HashMap<>();
        options.put("property-version", "1");
        options.put("connector", "postgres-cdc");
        options.put("hostname", "localhost");
        options.put("port", "3306");
        options.put("username", "test-user");
        options.put("database-name", "test-db");
        options.put("schema-name", "test-schema");
        options.put("table-name", "test-tbl");
        options.put("slot.name", "test-slot");
        options.put("scan.startup.mode", "initial");
        options.put("scan.incremental.snapshot.chunk.size", "100");
        return new OptionSnapshot(options);
    }

    @Override
    protected Map<String, String> getExpectedSourceMigratedOptions(
            ObjectIdentifier objectIdentifier) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "postgres-cdc");
        options.put("hostname", "localhost");
        options.put("port", "3307");
        options.put("username", "test-user-1");
        options.put("password", "test-password-1");
        options.put("database-name", "test-db");
        options.put("schema-name", "test-schema");
        options.put("table-name", "test-tbl");
        options.put("slot.name", "test-slot");
        options.put("scan.startup.mode", "initial");
        options.put("scan.incremental.snapshot.chunk.size", "100");
        return options;
    }

    @Override
    @Test
    public void testSinkOptionUpgrade() throws Exception {}

    @Override
    protected ObjectIdentifier createSinkTable() throws Exception {
        return null;
    }

    @Override
    protected DynamicTableSinkFactory getSinkFactory() {
        return null;
    }

    @Override
    protected OptionSnapshot getExpectedSinkSnapshot(ObjectIdentifier objectIdentifier) {
        return null;
    }

    @Override
    protected Map<String, String> getExpectedSinkMigratedOptions(
            ObjectIdentifier objectIdentifier) {
        return Collections.emptyMap();
    }
}
