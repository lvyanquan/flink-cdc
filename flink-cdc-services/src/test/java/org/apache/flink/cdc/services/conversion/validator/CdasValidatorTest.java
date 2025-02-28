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

package org.apache.flink.cdc.services.conversion.validator;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.utils.ServiceTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test cases for {@link Validator} with CDAS jobs. */
class CdasValidatorTest {

    @Test
    void testParseFullyQualifiedCdas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar",
                        "false",
                        "'invite_register_records'",
                        "",
                        "`OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_catalog_foo.sink_database_bar", "'extras' = 'something'");
    }

    @Test
    void testParseUsingSourceCatalogCdas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/cdas.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_database_bar",
                        "false",
                        "'invite_register_records'",
                        "",
                        "`OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_catalog_foo.sink_database_bar", "'extras' = 'something'");
    }

    @Test
    void testParseUsingSinkCatalogCdas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar",
                        "false",
                        "'invite_register_records'",
                        "",
                        "`OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_database_bar", "'extras' = 'something'");
    }

    @Test
    void testParseIncompleteCdas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-implicit-catalog.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_database_bar",
                        "false",
                        "'invite_register_records'",
                        "",
                        "`OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_catalog_foo.sink_database_bar", "'extras' = 'something'");
    }

    @Test
    void testParseCdasWithMultipleIncludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-with-multiple-including-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar",
                        "false",
                        "'devices|logouts|active_at_records|active_members|members|settings|invite_register_records|anchor_punishment_records'",
                        "",
                        "`OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1726792955000')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly(
                        "sink_catalog_foo.sink_database_bar", "'schemaname' = 'iwee_users'");
    }

    @Test
    void testParseCdasIncludingAllTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-all-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar",
                        "true",
                        "",
                        "",
                        "`OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_catalog_foo.sink_database_bar", "");
    }

    @Test
    void testParseCdasWithIncludingAndExcludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-and-excluding-tables.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asDatabaseName",
                        "isIncludingAllTables",
                        "includingTables",
                        "excludingTables",
                        "tableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar",
                        "false",
                        "'table_.*'",
                        "'table_42'",
                        "`OPTIONS`('server-id' = '2222-2222', 'server-time-zone' = 'UTC')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCdas)
                .extracting(ConvertibleNode::getNode)
                .extracting("sqlCreateDatabase")
                .extracting("databaseName", "propertyList")
                .map(Object::toString)
                .containsExactly("sink_catalog_foo.sink_database_bar", "");
    }

    @Test
    void testParseNonCtasNorCdas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/malformed/not-cxas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThatThrownBy(() -> Validator.parseConvertibleNode(sqlNode))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("No valid CTAS or CDAS statements found.");
    }
}
