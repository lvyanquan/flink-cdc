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

package org.apache.flink.cdc.services.conversion;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.conversion.factory.CxasToYamlConverterFactory;
import org.apache.flink.cdc.services.utils.ServiceTestUtils;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Integrated test cases for {@link CxasToYamlConverter}. */
class CxasToYamlConverterITCase {

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

    private static final CatalogOptionsInfo UNSUPPORTED_CATALOG_OPTIONS_INFO =
            new CatalogOptionsInfo(
                    ImmutableMap.of("type", "mongodb"),
                    ImmutableMap.of("type", "maxcompute"),
                    "source_default_db",
                    "sink_default_db");

    @Test
    void testConvertCtasToYaml() throws Exception {
        runGenericConversion(
                "cases/fully-qualified/ctas.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` WITH (\n"
                        + "#   'enableTypeNormalization' = 'true'\n"
                        + "# )\n"
                        + "# AS TABLE `source_catalog_foo`.`source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                        + "# /*+ `OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false') */\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  jdbc.properties.tinyInt1isBit: 'false'\n"
                        + "  jdbc.properties.transformedBitIsBoolean: 'false'\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  server-id: 5611-5620\n"
                        + "  tables: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n");

        runGenericConversion(
                "cases/using-source-catalog/ctas.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` WITH (\n"
                        + "#   'enableTypeNormalization' = 'true'\n"
                        + "# )\n"
                        + "# AS TABLE `source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                        + "# /*+ `OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false') */\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  jdbc.properties.tinyInt1isBit: 'false'\n"
                        + "  jdbc.properties.transformedBitIsBoolean: 'false'\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  server-id: 5611-5620\n"
                        + "  tables: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n");

        runGenericConversion(
                "cases/using-sink-catalog/ctas.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_database_bar`.`sink_table_baz` WITH (\n"
                        + "#   'enableTypeNormalization' = 'true'\n"
                        + "# )\n"
                        + "# AS TABLE `source_catalog_foo`.`source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                        + "# /*+ `OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false') */\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  jdbc.properties.tinyInt1isBit: 'false'\n"
                        + "  jdbc.properties.transformedBitIsBoolean: 'false'\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  server-id: 5611-5620\n"
                        + "  tables: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n");

        runGenericConversion(
                "cases/special/ctas-with-add-columns.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` WITH (\n"
                        + "#   'changelog-producer' = 'input',\n"
                        + "#   'enableTypeNormalization' = 'true',\n"
                        + "#   'write-buffer-size' = '128mb',\n"
                        + "#   'bucket' = '1'\n"
                        + "# )\n"
                        + "# AS TABLE `source_catalog_foo`.`source_database_bar`.`source_table_baz`\n"
                        + "# /*+ `OPTIONS`('scan.startup.mode' = 'initial') */\n"
                        + "# ADD COLUMN (\n"
                        + "#   `etl_load_ts` AS `now`(),\n"
                        + "#   `dt` AS COALESCE(CAST(`DATE_FORMAT`(`created_at`, 'yyyyMMdd') AS STRING), '__DEFAULT_PARTITION__') FIRST\n"
                        + "# )\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  scan.startup.mode: initial\n"
                        + "  tables: source_database_bar.source_table_baz\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_database_bar.source_table_baz\n"
                        + "transform:\n"
                        + "- projection: \\*, `now`() AS etl_load_ts, COALESCE(CAST(`DATE_FORMAT`(`created_at`,\n"
                        + "    'yyyyMMdd') AS STRING), '__DEFAULT_PARTITION__') AS dt\n"
                        + "  source-table: source_database_bar.source_table_baz\n");

        runGenericConversion(
                "cases/special/ctas-with-primary-key-and-partition-key.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` (\n"
                        + "#   PRIMARY KEY (`dt`, `id`) NOT ENFORCED\n"
                        + "# )\n"
                        + "# PARTITIONED BY (`dt`)\n"
                        + "# WITH (\n"
                        + "#   'changelog-producer' = 'input',\n"
                        + "#   'enableTypeNormalization' = 'true',\n"
                        + "#   'write-buffer-size' = '128mb',\n"
                        + "#   'bucket' = '1'\n"
                        + "# )\n"
                        + "# AS TABLE `source_catalog_foo`.`source_database_bar`.`source_table_baz`\n"
                        + "# /*+ `OPTIONS`('scan.startup.mode' = 'initial') */\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  scan.startup.mode: initial\n"
                        + "  tables: source_database_bar.source_table_baz\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_database_bar.source_table_baz\n"
                        + "transform:\n"
                        + "- primary-keys: dt,id\n"
                        + "  partition-keys: dt\n"
                        + "  source-table: source_database_bar.source_table_baz\n");

        runGenericConversion(
                "cases/special/ctas-implicit-catalog.sql",
                "# Converted from the following SQL: \n"
                        + "# \n"
                        + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` WITH (\n"
                        + "#   'enableTypeNormalization' = 'true'\n"
                        + "# )\n"
                        + "# AS TABLE `source_table_baz`\n"
                        + "# /*+ `OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false') */\n"
                        + "---\n"
                        + "source:\n"
                        + "  hostname: localhost\n"
                        + "  jdbc.properties.tinyInt1isBit: 'false'\n"
                        + "  jdbc.properties.transformedBitIsBoolean: 'false'\n"
                        + "  password: '12345678'\n"
                        + "  port: '3306'\n"
                        + "  server-id: 5611-5620\n"
                        + "  tables: source_default_db.source_table_baz\n"
                        + "  type: mysql\n"
                        + "  username: root\n"
                        + "sink:\n"
                        + "  endpoint: localhost:11037\n"
                        + "  password: '87654321'\n"
                        + "  type: hologres\n"
                        + "  username: admin\n"
                        + "route:\n"
                        + "- sink-table: sink_database_bar.sink_table_baz\n"
                        + "  source-table: source_default_db.source_table_baz\n");

        Assertions.assertThatThrownBy(
                        () ->
                                runGenericConversion(
                                        UNSUPPORTED_CATALOG_OPTIONS_INFO,
                                        "cases/malformed/ctas-unsupported.sql",
                                        null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "CDC YAML does not support migrating jobs with `mongodb` connector. "
                                + "Currently, only these connectors are supported: mysql, kafka, upsert-kafka, starrocks, hologres, paimon");

        Assertions.assertThatThrownBy(
                        () -> runGenericConversion("cases/malformed/not-cxas.sql", null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("No valid CTAS or CDAS statements found.");
    }

    @Test
    void testConvertCdasToYaml() throws Exception {
        runGenericConversion(
                "cases/fully-qualified/cdas.sql",
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

        runGenericConversion(
                "cases/using-source-catalog/cdas.sql",
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

        runGenericConversion(
                "cases/using-sink-catalog/cdas.sql",
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

        runGenericConversion(
                "cases/special/cdas-including-all-tables.sql",
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

        runGenericConversion(
                "cases/special/cdas-including-and-excluding-tables.sql",
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

        runGenericConversion(
                "cases/special/cdas-with-multiple-including-tables.sql",
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

        runGenericConversion(
                "cases/special/cdas-implicit-catalog.sql",
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

        Assertions.assertThatThrownBy(
                        () ->
                                runGenericConversion(
                                        UNSUPPORTED_CATALOG_OPTIONS_INFO,
                                        "cases/malformed/cdas-unsupported.sql",
                                        null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "CDC YAML does not support migrating jobs with `mongodb` connector. "
                                + "Currently, only these connectors are supported: mysql, kafka, upsert-kafka, starrocks, hologres, paimon");

        Assertions.assertThatThrownBy(
                        () -> runGenericConversion("cases/malformed/not-cxas.sql", null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("No valid CTAS or CDAS statements found.");
    }

    private void runGenericConversion(String sqlPath, String expectedYaml) throws Exception {
        runGenericConversion(CATALOG_OPTIONS_INFO, sqlPath, expectedYaml);
    }

    private void runGenericConversion(CatalogOptionsInfo info, String sqlPath, String expectedYaml)
            throws Exception {
        CxasToYamlConverter converter = CxasToYamlConverterFactory.createConverter();
        String sqlScript = ServiceTestUtils.getResourceContents(sqlPath);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(sqlScript);
        ConvertibleNode node = converter.validate(sqlNode);
        Assertions.assertThat(converter.convertToYaml(node, info)).isEqualTo(expectedYaml);
    }
}
