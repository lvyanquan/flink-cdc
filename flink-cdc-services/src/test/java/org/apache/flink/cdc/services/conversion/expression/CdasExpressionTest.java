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

package org.apache.flink.cdc.services.conversion.expression;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/** Test cases for {@link CdasExpression}. */
class CdasExpressionTest {

    @Test
    void testBuildMinimalExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSinkDatabase("sink_db")
                        .setOriginalDDL("CREATE DATABASE `sink_db` AS DATABASE `source_db`;")
                        .build();

        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE `sink_db` AS DATABASE `source_db`;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.\\.*\n"
                                + "sink: {}\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }

    @Test
    void testBuildFullExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL("A_LONG_LONG_LONG_LONG_AND_COMPLEX_CTAS_EXPRESSION;")
                        .setIncludeAllTables(false)
                        .setIncludedTables(Collections.singletonList("table_\\d+"))
                        .setExcludedTables(Collections.singletonList("table_42"))
                        .build();

        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# A_LONG_LONG_LONG_LONG_AND_COMPLEX_CTAS_EXPRESSION;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.table_\\d+\n"
                                + "  tables.exclude: source_db.table_42\n"
                                + "  type: mysql\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }

    @Test
    void testIncludingAllTablesExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL("<< INCLUDING ALL TABLES EXPRESSION >>;")
                        .setIncludeAllTables(true)
                        .build();
        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# << INCLUDING ALL TABLES EXPRESSION >>;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.\\.*\n"
                                + "  type: mysql\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }

    @Test
    void testIncludingSpecificTablesExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL("<< INCLUDING SPECIFIC TABLES EXPRESSION >>;")
                        .setIncludeAllTables(false)
                        .setIncludedTables(Arrays.asList("table_foo", "table_bar"))
                        .build();
        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# << INCLUDING SPECIFIC TABLES EXPRESSION >>;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.table_foo,source_db.table_bar\n"
                                + "  type: mysql\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }

    @Test
    void testExcludingSpecificTablesExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL("<< EXCLUDING SPECIFIC TABLES EXPRESSION >>;")
                        .setIncludeAllTables(false)
                        .setExcludedTables(Arrays.asList("table_fun", "table_baz"))
                        .build();
        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# << EXCLUDING SPECIFIC TABLES EXPRESSION >>;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.\\.*\n"
                                + "  tables.exclude: source_db.table_fun,source_db.table_baz\n"
                                + "  type: mysql\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }

    @Test
    void testIncludingAndExcludingSpecificTablesExpression() {
        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL("<< [IN/EX]CLUDING SPECIFIC TABLES EXPRESSION >>;")
                        .setIncludeAllTables(false)
                        .setIncludedTables(Collections.singletonList("table_.*"))
                        .setExcludedTables(Arrays.asList("table_fun", "table_baz"))
                        .build();
        Assertions.assertThat(expression.toYaml())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# << [IN/EX]CLUDING SPECIFIC TABLES EXPRESSION >>;\n"
                                + "---\n"
                                + "source:\n"
                                + "  tables: source_db.table_\\.*\n"
                                + "  tables.exclude: source_db.table_fun,source_db.table_baz\n"
                                + "  type: mysql\n"
                                + "sink:\n"
                                + "  type: paimon\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_db.<:>\n"
                                + "  source-table: source_db.\\.*\n");
    }
}
