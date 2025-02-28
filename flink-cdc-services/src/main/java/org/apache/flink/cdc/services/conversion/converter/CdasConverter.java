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
import org.apache.flink.cdc.services.conversion.expression.CdasExpression;
import org.apache.flink.cdc.services.conversion.migrator.Migrator;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractHintsAsOptions;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractPropertyListAsOptions;
import static org.apache.flink.cdc.services.conversion.utils.ConvertingUtils.extractTableNames;

/**
 * Specialized class for converting CDAS {@link ConvertibleNode} to equivalent CDC YAML expression.
 */
public class CdasConverter {

    private CdasConverter() {}

    /** Converts given CDAS node to CDC YAML. */
    public static String convertToYaml(
            ConvertibleNode node,
            IdentifierInfo identifierInfo,
            CatalogOptionsInfo catalogOptionsInfo) {
        SqlCreateDatabaseAs cdasNode = node.getCdasNode();

        List<String> sourceDatabaseId = identifierInfo.getSourceIdentifier();
        List<String> sinkDatabaseId = identifierInfo.getSinkIdentifier();

        Map<String, String> sourceOptions =
                new HashMap<>(catalogOptionsInfo.getSourceCatalogOptions());
        sourceOptions.putAll(extractHintsAsOptions(cdasNode.getTableHints()));
        sourceOptions = Migrator.migrateSource(sourceOptions);

        Map<String, String> sinkOptions = new HashMap<>(catalogOptionsInfo.getSinkCatalogOptions());
        sinkOptions.putAll(
                extractPropertyListAsOptions(cdasNode.getSqlCreateDatabase().getPropertyList()));
        sinkOptions = Migrator.migrateSink(sinkOptions);

        CdasExpression expression =
                new CdasExpression.Builder()
                        .setSourceDatabase(sourceDatabaseId.get(1))
                        .setSourceOptions(sourceOptions)
                        .setIncludedTables(extractTableNames(cdasNode.getIncludingTables()))
                        .setExcludedTables(extractTableNames(cdasNode.getExcludingTables()))
                        .setIncludeAllTables(cdasNode.isIncludingAllTables())
                        .setSinkDatabase(sinkDatabaseId.get(1))
                        .setSinkOptions(sinkOptions)
                        .setOriginalDDL(cdasNode.toString())
                        .build();

        return expression.toYaml();
    }
}
