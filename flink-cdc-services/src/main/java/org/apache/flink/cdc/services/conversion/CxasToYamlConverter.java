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

package org.apache.flink.cdc.services.conversion;

import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

import java.util.List;

/** Service interface for converting a CTAS / CXAS SQL script to CDC YAML. */
public interface CxasToYamlConverter {

    /** Validates if provided string is a valid and convertible CTAS / CDAS SQL script. */
    ConvertibleNode validate(List<Object> nodeList) throws Exception;

    /** Extract {@link IdentifierInfo} from given CTAS / CDAS SQL script. */
    IdentifierInfo parseIdentifiers(ConvertibleNode node) throws Exception;

    /** Converts given CXAS / CDAS SQL script to equivalent CDC YAML file. */
    String convertToYaml(ConvertibleNode node, CatalogOptionsInfo options) throws Exception;
}
