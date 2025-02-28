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

/** Test cases for {@link CtasConverter}. */
class CtasConverterTest {

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
    void testConvertFullyQualifiedCtasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }

    @Test
    void testConvertUsingSourceCatalogCtasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/ctas.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }

    @Test
    void testConvertUsingSinkCatalogCtasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }

    @Test
    void testConvertCtasWithAddColumns() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-add-columns.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }

    @Test
    void testConvertCtasWithPrimaryKeyAndPartitionKey() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-primary-key-and-partition-key.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }

    @Test
    void testConvertIncompleteCtasToYaml() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-implicit-catalog.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Converter.convertToYaml(node, CATALOG_OPTIONS_INFO))
                .isEqualTo(
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
    }
}
