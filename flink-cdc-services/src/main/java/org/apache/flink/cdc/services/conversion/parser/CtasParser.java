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
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlIdentifier;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractNamesFromIdentifier;

/** Specialized class for parsing traits from CTAS {@link ConvertibleNode}. */
public class CtasParser {

    private CtasParser() {}

    /** Extracts {@link IdentifierInfo} from a CTAS node. */
    public static IdentifierInfo parseIdentifierInfo(ConvertibleNode node) {
        Preconditions.checkArgument(node.isCtas(), "Expected a CTAS convertible node.");

        String catalogName = node.getUsingCatalogName();
        SqlCreateTableAs ctasNode = node.getCtasNode();
        List<String> sourceTableId =
                extractTableIdentifier(catalogName, ctasNode.getAsTableIdentifier());
        List<String> sinkTableId = extractTableIdentifier(catalogName, ctasNode.getTableName());
        return new IdentifierInfo(
                IdentifierInfo.IdentifierLevel.TABLE,
                sourceTableId,
                IdentifierInfo.IdentifierLevel.TABLE,
                sinkTableId);
    }

    /**
     * Tries to extract a complete 3-clause CTAS TableIdentifier from given {@link SqlIdentifier},
     * always returning a {@link List} with 3 elements. <br>
     *
     * <ul>
     *   <li>{@code components[0]} is a {@code @Nullable String}. <br>
     *       If there's neither explicit catalog name nor {@code USE CATALOG} statement, this field
     *       will be {@code null} and default catalog will be used.
     *   <li>{@code components[1]} is a {@code @Nullable String} for the database name. <br>
     *       If there's no explicit database name, this field will be {@code null} and default
     *       database name will be used.
     *   <li>{@code components[2]} is a {@code @NonNull String} for the table name.
     * </ul>
     */
    public static List<String> extractTableIdentifier(
            @Nullable String usingCatalogName, SqlIdentifier identifier) {
        List<String> tableId = extractNamesFromIdentifier(identifier);
        Preconditions.checkArgument(
                !tableId.isEmpty() && tableId.size() <= 3,
                "Invalid CTAS source table identifier: " + tableId);

        List<String> tableIdComponents = new ArrayList<>(tableId);

        if (tableIdComponents.size() == 3) {
            // It's a complete TableIdentifier, needless to do anything
        } else if (tableIdComponents.size() == 2) {
            // It's a TableIdentifier with implicit catalog name
            tableIdComponents.add(0, usingCatalogName);
        } else if (tableIdComponents.size() == 1) {
            // It's a table-only identifier, with implicit catalog name and default database name
            tableIdComponents.add(0, usingCatalogName);
            tableIdComponents.add(1, null);
        } else {
            throw new IllegalArgumentException(
                    "Invalid CTAS source table identifier: " + tableIdComponents);
        }

        return tableIdComponents;
    }
}
