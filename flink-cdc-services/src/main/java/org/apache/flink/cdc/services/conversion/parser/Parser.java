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
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

/** Proxy class for parsing traits from CTAS or CDAS {@link ConvertibleNode}. */
public class Parser {

    private Parser() {}

    /** Extracts {@link IdentifierInfo} from CTAS or CDAS node. */
    public static IdentifierInfo parseIdentifierInfo(ConvertibleNode node) {
        if (node.isCtas()) {
            return CtasParser.parseIdentifierInfo(node);
        } else if (node.isCdas()) {
            return CdasParser.parseIdentifierInfo(node);
        } else {
            throw new IllegalArgumentException(
                    "Only supports parsing CTAS and CDAS nodes, but is: " + node.getNodeType());
        }
    }
}
