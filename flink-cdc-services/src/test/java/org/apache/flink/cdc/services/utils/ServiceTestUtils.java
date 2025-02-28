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

package org.apache.flink.cdc.services.utils;

import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.apache.flink.table.planner.parse.CalciteParser;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Utility methods for testing CDC services. */
public class ServiceTestUtils {

    public static final Predicate<String> JAVA_COMMENT_FILTERER = (line) -> !line.startsWith("//");
    public static final Predicate<String> SQL_COMMENT_FILTERER = (line) -> !line.startsWith("--");
    public static final Predicate<String> YAML_COMMENT_FILTERER = (line) -> !line.startsWith("#");

    public static String getResourceContents(String resourcePath) throws IOException {
        return getResourceContents(resourcePath, (ignored) -> true);
    }

    private static final SqlParser.Config config =
            SqlParser.config()
                    .withParserFactory(FlinkSqlParserFactories.create(FlinkSqlConformance.DEFAULT))
                    .withConformance(FlinkSqlConformance.DEFAULT)
                    .withLex(Lex.MYSQL)
                    .withIdentifierMaxLength(256);
    private static final CalciteParser parser = new CalciteParser(config);

    public static List<Object> parseStmtList(String sql) {
        return new ArrayList<>(parser.parseStmtList(sql).getList());
    }

    public static String getResourceContents(String resourcePath, Predicate<String> commentFilter)
            throws IOException {

        try {
            URL resourceUrl = ServiceTestUtils.class.getClassLoader().getResource(resourcePath);
            Path path = Paths.get(Objects.requireNonNull(resourceUrl).toURI());
            return Files.readAllLines(path).stream()
                    .map(String::trim)
                    .filter(commentFilter)
                    .collect(Collectors.joining("\n"));
        } catch (NullPointerException | URISyntaxException e) {
            throw new IOException("Failed to retrieve resource @ " + resourcePath, e);
        }
    }

    public static ImmutableMap.Builder<String, String> buildMap() {
        return ImmutableMap.builder();
    }
}
