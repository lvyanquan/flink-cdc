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

package org.apache.flink.cdc.services.conversion.utils;

import org.apache.flink.cdc.services.conversion.validator.Validator;
import org.apache.flink.cdc.services.utils.ServiceTestUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.calcite.sql.SqlNodeList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Test cases for {@link ConvertingUtils}. */
class ConvertingUtilsTest {

    @Test
    void testAppendDdlAsComment() throws Exception {
        String originalDdl = "CREATE DATABASE `sink_db` AS DATABASE `source_db`;";
        Assertions.assertThat(ConvertingUtils.appendDdlAsComment(originalDdl))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE `sink_db` AS DATABASE `source_db`;\n"
                                + "---\n");

        String longOriginalDdl =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        Assertions.assertThat(ConvertingUtils.appendDdlAsComment(longOriginalDdl))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# \n"
                                + "# CREATE TABLE IF NOT EXISTS\n"
                                + "# `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz`\n"
                                + "# WITH (\n"
                                + "# 'enableTypeNormalization' = 'true'\n"
                                + "# )\n"
                                + "# AS TABLE\n"
                                + "# `source_catalog_foo`.`source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                                + "# /*+ `OPTIONS`(\n"
                                + "# 'server-id' = '5611-5620',\n"
                                + "# 'jdbc.properties.tinyInt1isBit' = 'false',\n"
                                + "# 'jdbc.properties.transformedBitIsBoolean' = 'false'\n"
                                + "# ) */\n"
                                + "---\n");
    }

    @Test
    void testDumpYaml() {
        String originalDdl = "CREATE DATABASE `sink_db` AS DATABASE `source_db`;";
        Map<String, Object> yamlMap =
                ImmutableMap.of(
                        "str-key",
                        "alice",
                        "int-key",
                        31415923,
                        "double-key",
                        1.618,
                        "boolean-key",
                        true,
                        "list-key",
                        Arrays.asList("a", "b", "c"),
                        "map-key",
                        Collections.singletonMap("k", "v"));

        Assertions.assertThat(ConvertingUtils.dumpYaml(originalDdl, yamlMap))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE `sink_db` AS DATABASE `source_db`;\n"
                                + "---\n"
                                + "str-key: alice\n"
                                + "int-key: 31415923\n"
                                + "double-key: 1.618\n"
                                + "boolean-key: true\n"
                                + "list-key:\n"
                                + "- a\n"
                                + "- b\n"
                                + "- c\n"
                                + "map-key:\n"
                                + "  k: v\n");
    }

    @Test
    void testQuoteDotInRegexp() {
        Assertions.assertThat(ConvertingUtils.quoteDotInRegexp("prefix_.*"))
                .isEqualTo("prefix_\\.*");
        Assertions.assertThat(ConvertingUtils.quoteDotInRegexp(".+_suffix"))
                .isEqualTo("\\.+_suffix");
        Assertions.assertThat(ConvertingUtils.quoteDotInRegexp("nothing")).isEqualTo("nothing");
    }

    @Test
    void testGenerateTableId() {
        Assertions.assertThat(ConvertingUtils.generateTableId("db", "tbl")).isEqualTo("db.tbl");
        Assertions.assertThat(ConvertingUtils.generateTableId("db_.+", "tbl_.*"))
                .isEqualTo("db_\\.+.tbl_\\.*");
        Assertions.assertThat(ConvertingUtils.generateTableId("db_only", null))
                .isEqualTo("db_only");
        Assertions.assertThat(ConvertingUtils.generateTableId(null, "table_only"))
                .isEqualTo("table_only");
    }

    @Test
    void testOrdered() {
        String originalDdl = "CREATE DATABASE `sink_db` AS DATABASE `source_db`;";
        Map<String, Object> yamlMap =
                ImmutableMap.of(
                        "str-key",
                        "alice",
                        "int-key",
                        31415923,
                        "double-key",
                        1.618,
                        "boolean-key",
                        true,
                        "list-key",
                        Arrays.asList("a", "b", "c"),
                        "map-key",
                        Collections.singletonMap("k", "v"));

        Assertions.assertThat(
                        ConvertingUtils.dumpYaml(originalDdl, ConvertingUtils.ordered(yamlMap)))
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE `sink_db` AS DATABASE `source_db`;\n"
                                + "---\n"
                                + "boolean-key: true\n"
                                + "double-key: 1.618\n"
                                + "int-key: 31415923\n"
                                + "list-key:\n"
                                + "- a\n"
                                + "- b\n"
                                + "- c\n"
                                + "map-key:\n"
                                + "  k: v\n"
                                + "str-key: alice\n");
    }

    @Test
    void testExtractHintsAsOptions() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlNodeList hints = Validator.parseConvertibleNode(sqlNode).getCtasNode().getAsTableHints();

        Assertions.assertThat(ConvertingUtils.extractHintsAsOptions(hints))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                "jdbc.properties.tinyInt1isBit",
                                "false",
                                "jdbc.properties.transformedBitIsBoolean",
                                "false",
                                "server-id",
                                "5611-5620"));
    }

    @Test
    void testExtractPropertyListAsOptions() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlNodeList plist = Validator.parseConvertibleNode(sqlNode).getCtasNode().getPropertyList();

        Assertions.assertThat(ConvertingUtils.extractPropertyListAsOptions(plist))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of("enableTypeNormalization", "true"));
    }

    @Test
    void testExtractTableNames() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-and-excluding-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlCreateDatabaseAs cdasNode = Validator.parseConvertibleNode(sqlNode).getCdasNode();
        Assertions.assertThat(ConvertingUtils.extractTableNames(cdasNode.getIncludingTables()))
                .containsExactly("table_.*");

        Assertions.assertThat(ConvertingUtils.extractTableNames(cdasNode.getExcludingTables()))
                .containsExactly("table_42");
    }

    @Test
    void testExtractCalculatedColumnsExpression() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-add-columns.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlCreateTableAs ctasNode = Validator.parseConvertibleNode(sqlNode).getCtasNode();
        Assertions.assertThat(
                        ConvertingUtils.extractCalculatedColumnsExpression(
                                ctasNode.getAsTableAddColumns()))
                .containsExactly(
                        "`now`() AS etl_load_ts",
                        "COALESCE(CAST(`DATE_FORMAT`(`created_at`, 'yyyyMMdd') AS STRING), '__DEFAULT_PARTITION__') AS dt");
    }

    @Test
    void testExtractPrimaryKeyConstraints() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-primary-key-and-partition-key.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlCreateTableAs ctasNode = Validator.parseConvertibleNode(sqlNode).getCtasNode();
        Assertions.assertThat(
                        ConvertingUtils.extractPrimaryKeyConstraints(ctasNode.getFullConstraints()))
                .containsExactly("dt", "id");
    }

    @Test
    void testExtractPartitionKey() throws Exception {
        String originalDDL =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-primary-key-and-partition-key.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(originalDDL);
        SqlCreateTableAs ctasNode = Validator.parseConvertibleNode(sqlNode).getCtasNode();
        Assertions.assertThat(ConvertingUtils.extractPartitionKey(ctasNode.getPartitionKeyList()))
                .containsExactly("dt");
    }
}
