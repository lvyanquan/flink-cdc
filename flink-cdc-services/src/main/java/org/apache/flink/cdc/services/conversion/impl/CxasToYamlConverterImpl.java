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

package org.apache.flink.cdc.services.conversion.impl;

import org.apache.flink.cdc.services.conversion.CxasToYamlConverter;
import org.apache.flink.cdc.services.conversion.context.ConvertibleNode;
import org.apache.flink.cdc.services.conversion.converter.Converter;
import org.apache.flink.cdc.services.conversion.parser.Parser;
import org.apache.flink.cdc.services.conversion.validator.Validator;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;

import java.util.List;

/** An implementation of {@link CxasToYamlConverter} service. */
public class CxasToYamlConverterImpl implements CxasToYamlConverter {

    @Override
    public ConvertibleNode validate(List<Object> nodeList) throws Exception {
        return Validator.parseConvertibleNode(nodeList);
    }

    @Override
    public IdentifierInfo parseIdentifiers(ConvertibleNode node) throws Exception {
        return Parser.parseIdentifierInfo(node);
    }

    @Override
    public String convertToYaml(ConvertibleNode node, CatalogOptionsInfo options) throws Exception {
        return Converter.convertToYaml(node, options);
    }
}
