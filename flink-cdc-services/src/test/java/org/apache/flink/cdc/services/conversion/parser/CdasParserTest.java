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

/** Test cases for {@link CdasParser}. */
class CdasParserTest {

    @Test
    void testParseFullyQualified() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseUsingSourceCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/cdas.sql",
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
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseUsingSinkCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/cdas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThat(Parser.parseIdentifierInfo(node))
                .extracting(
                        "sourceIdentifierLevel",
                        "sourceIdentifier",
                        "sinkIdentifierLevel",
                        "sinkIdentifier")
                .containsExactly(
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseCdasWithMultipleIncludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-with-multiple-including-tables.sql",
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
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseCdasIncludingAllTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-all-tables.sql",
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
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseCdasWithIncludingAndExcludingTables() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-including-and-excluding-tables.sql",
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
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("source_catalog_foo", "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseIncompleteCatalog() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/cdas-implicit-catalog.sql",
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
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList(null, "source_database_bar"),
                        IdentifierInfo.IdentifierLevel.DATABASE,
                        Arrays.asList("sink_catalog_foo", "sink_database_bar"));
    }

    @Test
    void testParseTypeMismatched() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);
        ConvertibleNode node = Validator.parseConvertibleNode(sqlNode);

        Assertions.assertThatThrownBy(() -> CdasParser.parseIdentifierInfo(node))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected a CDAS convertible node.");
    }
}
