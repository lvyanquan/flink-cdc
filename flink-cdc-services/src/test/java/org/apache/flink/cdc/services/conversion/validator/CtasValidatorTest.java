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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/** Test cases for {@link Validator} with CTAS jobs. */
class CtasValidatorTest {

    @Test
    void testParseFullyQualifiedCtas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/fully-qualified/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableIdentifier", "asTableHints", "asTableAddColumnAndPositions")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')",
                        "{}");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')");
    }

    @Test
    void testParseUsingSourceCatalogCtas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-source-catalog/ctas.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableIdentifier", "asTableHints", "asTableAddColumnAndPositions")
                .map(Object::toString)
                .containsExactly(
                        "source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')",
                        "{}");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')");
    }

    @Test
    void testParseUsingSinkCatalogCtas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/using-sink-catalog/ctas.sql", ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableIdentifier", "asTableHints", "asTableAddColumnAndPositions")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')",
                        "{}");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz_([0-9]|[0-9]{2})",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')");
    }

    @Test
    void testParseIncompleteCtas() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-implicit-catalog.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableIdentifier", "asTableHints", "asTableAddColumnAndPositions")
                .map(Object::toString)
                .containsExactly(
                        "source_table_baz",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')",
                        "{}");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_table_baz",
                        "`OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false')");
    }

    @Test
    void testParseCtasWithAddColumns() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-add-columns.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableIdentifier", "asTableHints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz",
                        "`OPTIONS`('scan.startup.mode' = 'initial')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTableAddColumnAndPositions")
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .extracting(Map::keySet)
                .asInstanceOf(InstanceOfAssertFactories.COLLECTION)
                .map(Object::toString)
                .containsExactlyInAnyOrder(
                        "`etl_load_ts` AS `now`()",
                        "`dt` AS COALESCE(CAST(`DATE_FORMAT`(`created_at`, 'yyyyMMdd') AS STRING), '__DEFAULT_PARTITION__')");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz",
                        "`OPTIONS`('scan.startup.mode' = 'initial')");
    }

    @Test
    void testParseCtasWithPrimaryKeyAndPartitionKey() throws Exception {
        String content =
                ServiceTestUtils.getResourceContents(
                        "cases/special/ctas-with-primary-key-and-partition-key.sql",
                        ServiceTestUtils.SQL_COMMENT_FILTERER);
        List<Object> sqlNode = ServiceTestUtils.parseStmtList(content);

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting(
                        "asTableIdentifier",
                        "asTableHints",
                        "asTableAddColumnAndPositions",
                        "partitionKeyList")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz",
                        "`OPTIONS`('scan.startup.mode' = 'initial')",
                        "{}",
                        "`dt`");

        Assertions.assertThat(Validator.parseConvertibleNode(sqlNode))
                .matches(ConvertibleNode::isCtas)
                .extracting(ConvertibleNode::getNode)
                .extracting("asTable")
                .extracting("tableName", "hints")
                .map(Object::toString)
                .containsExactly(
                        "source_catalog_foo.source_database_bar.source_table_baz",
                        "`OPTIONS`('scan.startup.mode' = 'initial')");
    }
}
