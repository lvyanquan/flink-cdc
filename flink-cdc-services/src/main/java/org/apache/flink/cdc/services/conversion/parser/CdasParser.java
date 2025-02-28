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
import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlIdentifier;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractNamesFromIdentifier;

/** Specialized class for parsing traits from CDAS {@link ConvertibleNode}. */
public class CdasParser {

    private CdasParser() {}

    /** Extracts {@link IdentifierInfo} from a CDAS node. */
    public static IdentifierInfo parseIdentifierInfo(ConvertibleNode node) {
        Preconditions.checkArgument(node.isCdas(), "Expected a CDAS convertible node.");

        String catalogName = node.getUsingCatalogName();
        SqlCreateDatabaseAs cdasNode = node.getCdasNode();
        List<String> sourceDatabaseId =
                extractDatabaseIdentifier(
                        catalogName, extractNamesFromIdentifier(cdasNode.getAsDatabaseName()));
        List<String> sinkDatabaseId =
                extractDatabaseIdentifier(
                        catalogName,
                        extractNamesFromIdentifier(
                                cdasNode.getSqlCreateDatabase().getDatabaseName()));
        return new IdentifierInfo(
                IdentifierInfo.IdentifierLevel.DATABASE,
                sourceDatabaseId,
                IdentifierInfo.IdentifierLevel.DATABASE,
                sinkDatabaseId);
    }

    /**
     * Tries to extract a complete 2-clause CDAS TableIdentifier from given {@link SqlIdentifier},
     * always returning a {@link List} with 2 elements. <br>
     *
     * <ul>
     *   <li>{@code components[0]} is a {@code @Nullable String}. <br>
     *       If there's neither explicit catalog name nor {@code USE CATALOG} statement, this field
     *       will be {@code null} and default catalog will be used.
     *   <li>{@code components[1]} is a {@code @NonNull String} for the database name.
     * </ul>
     */
    public static List<String> extractDatabaseIdentifier(
            @Nullable String usingCatalogName, List<String> databaseId) {
        Preconditions.checkArgument(
                !databaseId.isEmpty() && databaseId.size() <= 2,
                "Invalid CDAS source table identifier: " + databaseId);

        List<String> databaseIdComponents = new ArrayList<>(databaseId);

        if (databaseIdComponents.size() == 2) {
            // It's a complete DatabaseIdentifier, needless to do anything
        } else if (databaseIdComponents.size() == 1) {
            // It must be a DatabaseIdentifier with implicit catalog name
            databaseIdComponents.add(0, usingCatalogName);
        } else {
            throw new IllegalArgumentException(
                    "Invalid CDAS source table identifier: " + databaseIdComponents);
        }

        return databaseIdComponents;
    }
}
