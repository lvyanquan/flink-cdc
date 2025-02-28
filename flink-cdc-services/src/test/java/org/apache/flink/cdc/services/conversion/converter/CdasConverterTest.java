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

package org.apache.flink.cdc.services.conversion.converter;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.conversion.validator.Validator;
import org.apache.flink.cdc.services.utils.ServiceTestUtils;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test cases for {@link CdasConverter}. */
class CdasConverterTest {

    private static final CatalogOptionsInfo CATALOG_OPTIONS_INFO =
            new CatalogOptionsInfo(
                    ImmutableMap.of(
                            "type", "mysql",
                            "hostname", "localhost",
                            "port", "3306",
                            "username", "root",
                            "password", "12345678"),
                    ImmutableMap.of(
                            "type", "hologres",
                            "endpoint", "localhost:11037",
                            "username", "admin",
                            "password", "87654321"),
                    "source_default_db",
                    "sink_default_db");

    @Test
    void testConvertFullyQualifiedCdasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` WITH (\n"
                                + "#   'extras' = 'something'\n"
                                + "# ) AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING TABLE 'invite_register_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1728435528000'\n"
                                + "  server-id: 7777-7777\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.invite_register_records\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    @Test
    void testConvertUsingSourceCatalogCdasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/cdas.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` WITH (\n"
                                + "#   'extras' = 'something'\n"
                                + "# ) AS DATABASE `source_database_bar`\n"
                                + "# INCLUDING TABLE 'invite_register_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1728435528000'\n"
                                + "  server-id: 7777-7777\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.invite_register_records\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n"
                                + "");
    }

    @Test
    void testConvertUsingSinkCatalogCdasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_database_bar` WITH (\n"
                                + "#   'extras' = 'something'\n"
                                + "# ) AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING TABLE 'invite_register_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1728435528000'\n"
                                + "  server-id: 7777-7777\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.invite_register_records\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    @Test
    void testConvertCdasWithMultipleIncludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-with-multiple-including-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` WITH (\n"
                                + "#   'schemaname' = 'iwee_users'\n"
                                + "# ) AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING TABLE 'devices|logouts|active_at_records|active_members|members|settings|invite_register_records|anchor_punishment_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1726792955000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1726792955000'\n"
                                + "  server-id: 2222-2222\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.devices|logouts|active_at_records|active_members|members|settings|invite_register_records|anchor_punishment_records\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    @Test
    void testConvertCdasIncludingAllTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-all-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING ALL TABLES\n"
                                + "# /*+ `OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  server-id: 2222-2222\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.\\.*\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    @Test
    void testConvertCdasWithIncludingAndExcludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-and-excluding-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING TABLE 'table_.*'\n"
                                + "# EXCLUDING TABLE 'table_42'\n"
                                + "# /*+ `OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  server-id: 2222-2222\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.table_\\.*\n"
                                + "  tables.exclude: source_database_bar.table_42\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    @Test
    void testConvertIncompleteCdasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-implicit-catalog.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` WITH (\n"
                                + "#   'extras' = 'something'\n"
                                + "# ) AS DATABASE `source_database_bar`\n"
                                + "# INCLUDING TABLE 'invite_register_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  hostname: localhost\n"
                                + "  password: '12345678'\n"
                                + "  port: '3306'\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1728435528000'\n"
                                + "  server-id: 7777-7777\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.invite_register_records\n"
                                + "  type: mysql\n"
                                + "  username: root\n"
                                + "sink:\n"
                                + "  endpoint: localhost:11037\n"
                                + "  password: '87654321'\n"
                                + "  type: hologres\n"
                                + "  username: admin\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }
}
