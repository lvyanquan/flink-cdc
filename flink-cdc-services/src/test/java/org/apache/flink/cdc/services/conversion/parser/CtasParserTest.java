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

package org.apache.flink.cdc.services.conversion.parser;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.conversion.validator.Validator;
import org.apache.flink.cdc.services.utils.ServiceTestUtils;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test cases for {@link CtasParser}. */
class CtasParserTest {

    @Test
    void testParseFullyQualified() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(
                                "source_catalog_foo",
                                "source_database_bar",
                                "source_table_baz_([0-9]|[0-9]{2})"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseUsingSourceCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/ctas.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(
                                "source_catalog_foo",
                                "source_database_bar",
                                "source_table_baz_([0-9]|[0-9]{2})"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseUsingSinkCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(
                                "source_catalog_foo",
                                "source_database_bar",
                                "source_table_baz_([0-9]|[0-9]{2})"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseCtasWithAddColumns() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-add-columns.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(
                                "source_catalog_foo", "source_database_bar", "source_table_baz"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseCtasWithPrimaryKeyAndPartitionKey() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-primary-key-and-partition-key.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(
                                "source_catalog_foo", "source_database_bar", "source_table_baz"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseIncompleteCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-implicit-catalog.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList(null, null, "source_table_baz"),
                        IdentifierInfo.IdentifierLevel.TABLE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar", "sink_table_baz"));
    }

    @Test
    void testParseTypeMismatched() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThatThrownBy(() -> CtasParser.parseIdentifierInfo(node))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected a CTAS convertible node.");
    }
}
