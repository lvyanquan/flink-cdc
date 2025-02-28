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
import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlUseCatalog;

import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.List;

/** Extracts {@link ConvertibleNode} from given SQL scripts. */
public class Validator {

    private Validator() {}

    /** Extract a CTAS or CDAS {@link SqlNode} object that is applicable of conversion. */
    public static ConvertibleNode parseConvertibleNode(List<Object> nodeList) {
        @Nullable String usingCatalogName = null;

        for (Object node : nodeList) {
            if (node instanceof SqlUseCatalog) {
                // USE CATALOG <...> statement
                SqlUseCatalog sqlUseCatalog = (SqlUseCatalog) node;
                usingCatalogName = sqlUseCatalog.catalogName();
            } else if (node instanceof SqlCreateTableAs) {
                // CREATE TABLE ... AS TABLE ... statement
                return new ConvertibleNode(
                        (SqlCreateTableAs) node, ConvertibleNode.Type.CTAS, usingCatalogName);
            } else if (node instanceof SqlCreateDatabaseAs) {
                // CREATE DATABASE ... AS DATABASE ... statement
                return new ConvertibleNode(
                        (SqlCreateDatabaseAs) node, ConvertibleNode.Type.CDAS, usingCatalogName);
            } else if (!(node instanceof SqlNode)) {
                throw new IllegalArgumentException("Unexpected SqlNode class: " + node.getClass());
            }
        }
        throw new IllegalArgumentException("No valid CTAS or CDAS statements found.");
    }
}
