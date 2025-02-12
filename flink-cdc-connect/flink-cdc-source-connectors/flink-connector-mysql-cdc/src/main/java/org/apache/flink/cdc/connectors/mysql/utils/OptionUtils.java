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

package org.apache.flink.cdc.connectors.mysql.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** A utility class to print configuration of connectors. */
public class OptionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OptionUtils.class);

    /** Utility class can not be instantiated. */
    private OptionUtils() {}

    public static void printOptions(String identifier, Map<String, String> config) {
        Map<String, String> hideMap = ConfigurationUtils.hideSensitiveValues(config);
        LOG.info("Print {} connector configuration:", identifier);
        for (String key : hideMap.keySet()) {
            LOG.info("{} = {}", key, hideMap.get(key));
        }
    }

    private static final String TABLE_DYNAMIC_OPTION_PREFIX = "table.dynamic.option.";

    public static Map<String, String> getTableOptionsFromTableConfig(
            DynamicTableFactory.Context context) {

        Map<String, String> optionsFromTableConfig = new HashMap<>();
        Map<String, String> tableConfiguration;
        if (context.getConfiguration() instanceof Configuration) {
            tableConfiguration = ((Configuration) context.getConfiguration()).toMap();
        } else if (context.getConfiguration() instanceof TableConfig) {
            tableConfiguration =
                    ((TableConfig) context.getConfiguration()).getConfiguration().toMap();
        } else {
            throw new IllegalStateException(
                    "context.getConfiguration() should return either a Configuration type or a TableConfig type.");
        }

        tableConfiguration
                .keySet()
                .forEach(
                        (option) -> {
                            if (!option.startsWith(TABLE_DYNAMIC_OPTION_PREFIX)) {
                                return;
                            }

                            int left = TABLE_DYNAMIC_OPTION_PREFIX.length();
                            int right;

                            for (int i = 0; i < 3; ++i) {
                                right = seekNextCharacter(option, '.', left);
                                if (right < 0) {
                                    return;
                                }
                                String identifierInConfig = option.substring(left, right);
                                String identifierOfTable = null;
                                switch (i) {
                                    case 0:
                                        identifierOfTable =
                                                context.getObjectIdentifier().getCatalogName();
                                        break;
                                    case 1:
                                        identifierOfTable =
                                                context.getObjectIdentifier().getDatabaseName();
                                        break;
                                    case 2:
                                        identifierOfTable =
                                                context.getObjectIdentifier().getObjectName();
                                        break;
                                }
                                if (!identifierInConfig.equals("*")
                                        && !identifierInConfig.equals(identifierOfTable)) {
                                    return;
                                }
                                left = right + 1;
                            }

                            String trimmedKey = option.substring(left);
                            optionsFromTableConfig.put(trimmedKey, tableConfiguration.get(option));
                        });

        return optionsFromTableConfig;
    }

    private static int seekNextCharacter(String string, char character, int start) {
        for (int i = start; i < string.length(); ++i) {
            if (string.charAt(i) == character) {
                return i;
            }
        }
        return -1;
    }
}
