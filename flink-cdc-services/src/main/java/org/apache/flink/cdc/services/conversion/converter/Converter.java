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
import org.apache.flink.cdc.services.conversion.parser.Parser;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

/**
 * Proxy class for converting CTAS or CDAS {@link ConvertibleNode} to equivalent CDC YAML
 * expression.
 */
public class Converter {

    private Converter() {}

    /** Converts given CTAS or CDAS node to CDC YAML. */
    public static String convertToYaml(
            ConvertibleNode node, CatalogOptionsInfo catalogOptionsInfo) {
        IdentifierInfo identifierInfo = Parser.parseIdentifierInfo(node);
        if (node.isCtas()) {
            return CtasConverter.convertToYaml(node, identifierInfo, catalogOptionsInfo);
        } else if (node.isCdas()) {
            return CdasConverter.convertToYaml(node, identifierInfo, catalogOptionsInfo);
        } else {
            throw new IllegalArgumentException(
                    "Only supports converting CTAS and CDAS nodes, but is: " + node.getNodeType());
        }
    }
}
