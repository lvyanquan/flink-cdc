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

/** Test cases for {@link CdasExpression.Builder}. */
class CdasExpressionBuilderTest {

    @Test
    void testBuildMinimalExpression() {
        String originalDDL = "CREATE DATABASE `sink_db` AS DATABASE `source_db`;";
        CdasExpression expected =
                new CdasExpression(
                        "source_db",
                        Collections.emptyMap(),
                        true,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        "sink_db",
                        Collections.emptyMap(),
                        originalDDL);

        CdasExpression actual =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSinkDatabase("sink_db")
                        .setOriginalDDL(originalDDL)
                        .build();

        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testBuildFullExpression() {
        String originalDDL = "A_LONG_LONG_LONG_LONG_AND_COMPLEX_CTAS_EXPRESSION;";
        CdasExpression expected =
                new CdasExpression(
                        "source_db",
                        Collections.singletonMap("type", "mysql"),
                        false,
                        Collections.singletonList("table_\\d+"),
                        Collections.singletonList("table_42"),
                        "sink_db",
                        Collections.singletonMap("type", "paimon"),
                        originalDDL);

        CdasExpression actual =
                new CdasExpression.Builder()
                        .setSourceDatabase("source_db")
                        .setSourceOptions(Collections.singletonMap("type", "mysql"))
                        .setSinkDatabase("sink_db")
                        .setSinkOptions(Collections.singletonMap("type", "paimon"))
                        .setOriginalDDL(originalDDL)
                        .setIncludeAllTables(false)
                        .setIncludedTables(Collections.singletonList("table_\\d+"))
                        .setExcludedTables(Collections.singletonList("table_42"))
                        .build();

        Assertions.assertThat(actual).isEqualTo(expected);
    }
}
