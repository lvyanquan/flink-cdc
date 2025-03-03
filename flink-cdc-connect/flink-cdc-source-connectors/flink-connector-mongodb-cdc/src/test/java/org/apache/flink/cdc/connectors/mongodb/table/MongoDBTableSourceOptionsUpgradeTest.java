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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.OptionSnapshot;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.planner.runtime.stream.sql.OptionUpgradeTestBase;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Map;

/** Test cases for upgrading options of MongoDB CDC connector. */
public class MongoDBTableSourceOptionsUpgradeTest extends OptionUpgradeTestBase {

    @Override
    protected Map<String, String> getEnrichmentOptions() {
        return ImmutableMap.<String, String>builder()
                .put("connector", "mongodb")
                .put("hosts", "localhost")
                .put("scheme", "mongodb+srv")
                .put("username", "root")
                .put("password", "${new.secret.password}")
                .put("database", "testdb")
                .put("collection", "testcoll")
                .put("connection.options", "connectTimeoutMS=23000&socketTimeoutMS=24000")
                .put("scan.incremental.snapshot.enabled", "true")
                .put("scan.startup.mode", "initial")
                .build();
    }

    @Override
    protected ObjectIdentifier createSourceTable() {
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE source_tbl ("
                        + "  _id BIGINT,"
                        + "  PRIMARY KEY (_id) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'mongodb', "
                        + "  'hosts' = '127.0.0.1:27017', "
                        + "  'scheme' = 'mongodb', "
                        + "  'username' = 'admin', "
                        + "  'password' = '${secret.password}', "
                        + "  'database' = 'testdb', "
                        + "  'collection' = 'testcoll', "
                        + "  'connection.options' = 'connectTimeoutMS=12000&socketTimeoutMS=13000', "
                        + "  'scan.startup.mode' = 'initial', "
                        + "  'scan.incremental.snapshot.enabled' = 'true', "
                        + "  'scan.full-changelog' = 'true', "
                        + "  'scan.flatten-nested-columns.enabled' = 'true', "
                        + "  'scan.primitive-as-string' = 'true'"
                        + ");");
        return ObjectIdentifier.of(
                tableEnv.getCurrentCatalog(), tableEnv.getCurrentDatabase(), "source_tbl");
    }

    @Override
    protected DynamicTableSourceFactory getSourceFactory() {
        return new MongoDBTableSourceFactory();
    }

    @Override
    protected OptionSnapshot getExpectedSourceSnapshot(ObjectIdentifier objectIdentifier) {
        return new OptionSnapshot(
                ImmutableMap.<String, String>builder()
                        .put("collection", "testcoll")
                        .put("connection.options", "connectTimeoutMS=12000&socketTimeoutMS=13000")
                        .put("connector", "mongodb")
                        .put("database", "testdb")
                        .put("hosts", "127.0.0.1:27017")
                        .put("property-version", "1")
                        .put("scan.flatten-nested-columns.enabled", "true")
                        .put("scan.full-changelog", "true")
                        .put("scan.incremental.snapshot.enabled", "true")
                        .put("scan.primitive-as-string", "true")
                        .put("scan.startup.mode", "initial")
                        .put("scheme", "mongodb")
                        .put("username", "admin")
                        .build());
    }

    @Override
    protected Map<String, String> getExpectedSourceMigratedOptions(
            ObjectIdentifier objectIdentifier) {
        return ImmutableMap.<String, String>builder()
                .put("collection", "testcoll")
                .put("connection.options", "connectTimeoutMS=23000&socketTimeoutMS=24000")
                .put("connector", "mongodb")
                .put("database", "testdb")
                .put("hosts", "localhost")
                .put("password", "${new.secret.password}")
                .put("scan.flatten-nested-columns.enabled", "true")
                .put("scan.full-changelog", "true")
                .put("scan.incremental.snapshot.enabled", "true")
                .put("scan.primitive-as-string", "true")
                .put("scan.startup.mode", "initial")
                .put("scheme", "mongodb+srv")
                .put("username", "root")
                .build();
    }

    @Override
    @Test
    @Disabled("Mongo CDC is not meant to be used as a sink.")
    public void testSinkOptionUpgrade() {
        // Do nothing; Mongo CDC is not meant to be used as a sink.
    }

    @Override
    protected ObjectIdentifier createSinkTable() {
        throw new UnsupportedOperationException("Mongo CDC is not meant to be used as a sink.");
    }

    @Override
    protected DynamicTableSinkFactory getSinkFactory() {
        throw new UnsupportedOperationException("Mongo CDC is not meant to be used as a sink.");
    }

    @Override
    protected OptionSnapshot getExpectedSinkSnapshot(ObjectIdentifier objectIdentifier) {
        throw new UnsupportedOperationException("Mongo CDC is not meant to be used as a sink.");
    }

    @Override
    protected Map<String, String> getExpectedSinkMigratedOptions(
            ObjectIdentifier objectIdentifier) {
        throw new UnsupportedOperationException("Mongo CDC is not meant to be used as a sink.");
    }
}
