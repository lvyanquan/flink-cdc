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

import java.util.Collections;

/** Test cases for {@link CtasExpression.Builder}. */
class CtasExpressionBuilderTest {

    @Test
    void testBuildMinimalExpression() {
        String originalDDL = "CREATE TABLE `sink_db`.`sink_tbl` AS TABLE `source_db`.`source_tbl`;";
        CtasExpression expected =
                new CtasExpression(
                        "source_db",
                        "source_tbl",
                        Collections.emptyMap(),
                        "sink_db",
                        "sink_tbl",
                        Collections.emptyMap(),
                        originalDDL,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());

        CtasExpression actual =
                new CtasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceTable("source_tbl")
                        .setSinkDatabase("sink_db")
                        .setSinkTable("sink_tbl")
                        .setOriginalDDL(originalDDL)
                        .build();

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testBuildFullExpression() {
        String originalDDL = "A_LONG_LONG_LONG_LONG_AND_COMPLEX_CTAS_EXPRESSION;";
        CtasExpression expected =
                new CtasExpression(
                        "source_db",
                        "source_tbl",
                        Collections.singletonMap("type", "mysql"),
                        "sink_db",
                        "sink_tbl",
                        Collections.singletonMap("type", "paimon"),
                        originalDDL,
                        Collections.singletonList("id + 1 AS new_id"),
                        Collections.singletonList("id"),
                        Collections.singletonList("id,new_id"));

        CtasExpression actual =
                new CtasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceTable("source_tbl")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkTable("sink_tbl")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL(originalDDL)
                        .setCalculatedColumns(Collections.singletonList("id + 1 AS new_id"))
                        .setPrimaryKeys(Collections.singletonList("id"))
                        .setPartitionKeys(Collections.singletonList("id,new_id"))
                        .build();

        Assertions.assertThat(actual).isEqualTo(expected);
    }
}
