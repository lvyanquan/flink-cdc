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

package org.apache.flink.cdc.services.conversion.context;

import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlDistribution;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Test cases for {@link ConvertibleNode}. */
class ConvertibleNodeTest {

    private static final SqlCreateTableAs CTAS_NODE =
            new SqlCreateTableAs(
                    SqlParserPos.ZERO,
                    new SqlIdentifier("dummy_table_sink", SqlParserPos.ZERO),
                    SqlNodeList.EMPTY,
                    Collections.emptyList(),
                    SqlNodeList.EMPTY,
                    new SqlDistribution(SqlParserPos.ZERO, null, null, null),
                    SqlNodeList.EMPTY,
                    null,
                    null,
                    null,
                    false,
                    true,
                    null,
                    null,
                    null);

    private static final SqlCreateDatabaseAs CDAS_NODE =
            new SqlCreateDatabaseAs(
                    new SqlCreateDatabase(
                            SqlParserPos.ZERO,
                            new SqlIdentifier("dummy_database_source", SqlParserPos.ZERO),
                            SqlNodeList.EMPTY,
                            null,
                            true),
                    new SqlIdentifier("dummy_database_sink", SqlParserPos.ZERO),
                    true,
                    null,
                    null,
                    SqlNodeList.EMPTY);

    @Test
    void testConstructingCtasConvertibleNode() {
        ConvertibleNode node = new ConvertibleNode(CTAS_NODE, ConvertibleNode.Type.CTAS, null);

        Assertions.assertThat(node)
                .matches(ConvertibleNode::isCtas)
                .extracting("node", "nodeType", "usingCatalogName")
                .containsExactly(CTAS_NODE, ConvertibleNode.Type.CTAS, null);
    }

    @Test
    void testConstructingCdasConvertibleNode() {
        ConvertibleNode node = new ConvertibleNode(CDAS_NODE, ConvertibleNode.Type.CDAS, null);

        Assertions.assertThat(node)
                .matches(ConvertibleNode::isCdas)
                .extracting("node", "nodeType", "usingCatalogName")
                .containsExactly(CDAS_NODE, ConvertibleNode.Type.CDAS, null);
    }

    @Test
    void testConstructingCtasConvertibleNodeWithMismatchedType() {
        Assertions.assertThatThrownBy(
                        () -> new ConvertibleNode(CTAS_NODE, ConvertibleNode.Type.CDAS, null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected a SqlCreateDatabaseAs node for CDAS convertible type.");
    }

    @Test
    void testConstructingCdasConvertibleNodeWithMismatchedType() {
        Assertions.assertThatThrownBy(
                        () -> new ConvertibleNode(CDAS_NODE, ConvertibleNode.Type.CTAS, null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected a SqlCreateTableAs node for CTAS convertible type.");
    }
}
