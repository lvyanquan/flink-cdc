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

package org.apache.flink.cdc.services.conversion.converter;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.conversion.expression.CtasExpression;
import org.apache.flink.cdc.services.conversion.migrator.Migrator;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractCalculatedColumnsExpression;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractHintsAsOptions;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractPartitionKey;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractPrimaryKeyConstraints;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractPropertyListAsOptions;

/**
 * Specialized class for converting CTAS {@link ConvertibleNode} to equivalent CDC YAML expression.
 */
public class CtasConverter {

    private CtasConverter() {}

    /** Converts given CTAS node to CDC YAML. */
    public static String convertToYaml(
            ConvertibleNode node,
            IdentifierInfo identifierInfo,
            CatalogOptionsInfo catalogOptionsInfo) {
        SqlCreateTableAs ctasNode = node.getCtasNode();

        List<String> sourceTableId =
                fillDefaultDatabase(
                        identifierInfo.getSourceIdentifier(),
                        catalogOptionsInfo.getSourceDefaultDatabase());
        List<String> sinkTableId =
                fillDefaultDatabase(
                        identifierInfo.getSinkIdentifier(),
                        catalogOptionsInfo.getSinkDefaultDatabase());

        Map<String, String> sourceOptions =
                new HashMap<>(catalogOptionsInfo.getSourceCatalogOptions());
        sourceOptions.putAll(extractHintsAsOptions(ctasNode.getAsTableHints()));
        sourceOptions = Migrator.migrateSource(sourceOptions);

        Map<String, String> sinkOptions = new HashMap<>(catalogOptionsInfo.getSinkCatalogOptions());
        sinkOptions.putAll(extractPropertyListAsOptions(ctasNode.getPropertyList()));
        sinkOptions = Migrator.migrateSink(sinkOptions);

        CtasExpression expression =
                new CtasExpression.Builder()
                        .setSourceDatabase(sourceTableId.get(1))
                        .setSourceTable(sourceTableId.get(2))
                        .setSourceOptions(sourceOptions)
                        .setSinkDatabase(sinkTableId.get(1))
                        .setSinkTable(sinkTableId.get(2))
                        .setSinkOptions(sinkOptions)
                        .setCalculatedColumns(
                                extractCalculatedColumnsExpression(ctasNode.getAsTableAddColumns()))
                        .setPrimaryKeys(extractPrimaryKeyConstraints(ctasNode.getFullConstraints()))
                        .setPartitionKeys(extractPartitionKey(ctasNode.getPartitionKeyList()))
                        .setOriginalDDL(ctasNode.toString())
                        .build();
        return expression.toYaml();
    }

    private static List<String> fillDefaultDatabase(
            List<String> identifier, @Nullable String defaultDatabase) {
        if (identifier.get(1) == null) {
            identifier.set(1, defaultDatabase);
        }
        return identifier;
    }
}
