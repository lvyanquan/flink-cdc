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

import org.apache.flink.sql.parser.ddl.SqlCreateDatabaseAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.Objects;

/** A CTAS or CDAS snippet that is convertible to YAML. */
public class ConvertibleNode {
    private final SqlNode node;
    private final Type nodeType;
    private final @Nullable String usingCatalogName;

    public ConvertibleNode(SqlNode node, Type nodeType, @Nullable String usingCatalogName) {
        this.node = node;
        this.nodeType = nodeType;

        if (nodeType == Type.CTAS) {
            Preconditions.checkArgument(
                    node instanceof SqlCreateTableAs,
                    "Expected a SqlCreateTableAs node for CTAS convertible type.");
        } else if (nodeType == Type.CDAS) {
            Preconditions.checkArgument(
                    node instanceof SqlCreateDatabaseAs,
                    "Expected a SqlCreateDatabaseAs node for CDAS convertible type.");
        }

        this.usingCatalogName = usingCatalogName;
    }

    public SqlNode getNode() {
        return node;
    }

    public Type getNodeType() {
        return nodeType;
    }

    public boolean isCtas() {
        return nodeType == Type.CTAS;
    }

    public boolean isCdas() {
        return nodeType == Type.CDAS;
    }

    public SqlCreateTableAs getCtasNode() {
        Preconditions.checkArgument(isCtas(), "Convertible node is not a CTAS node");
        return (SqlCreateTableAs) node;
    }

    public SqlCreateDatabaseAs getCdasNode() {
        Preconditions.checkArgument(isCdas(), "Convertible node is not a CDAS node");
        return (SqlCreateDatabaseAs) node;
    }

    @Nullable
    public String getUsingCatalogName() {
        return usingCatalogName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConvertibleNode that = (ConvertibleNode) o;
        return Objects.equals(node, that.node)
                && nodeType == that.nodeType
                && Objects.equals(usingCatalogName, that.usingCatalogName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, nodeType, usingCatalogName);
    }

    @Override
    public String toString() {
        return "ConvertibleScript{"
                + "node="
                + node
                + ", nodeType="
                + nodeType
                + ", usingCatalogName='"
                + usingCatalogName
                + '\''
                + '}';
    }

    /**
     * Convertible {@link SqlNode} type. <br>
     * {@code CTAS} for {@link SqlCreateTableAs}. <br>
     * {@code CDAS} for {@link SqlCreateDatabaseAs}.
     */
    public enum Type {
        CTAS,
        CDAS
    }
}
