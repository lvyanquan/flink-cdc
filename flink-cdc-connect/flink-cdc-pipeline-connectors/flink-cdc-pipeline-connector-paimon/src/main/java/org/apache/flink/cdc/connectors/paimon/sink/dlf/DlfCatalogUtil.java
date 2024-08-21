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

package org.apache.flink.cdc.connectors.paimon.sink.dlf;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.dlf.DlfOptions;
import org.apache.flink.util.StringUtils;

import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class for Dlf catalog. */
public class DlfCatalogUtil {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DlfCatalogUtil.class);

    public static void convertOptionToDlf(Options catalogOptions, ReadableConfig flinkConfig) {
        if (catalogOptions.containsKey("metastore")
                && catalogOptions.get("metastore").equals("dlf-paimon")) {
            LOGGER.debug("Adding option for dlf-paimon catalog");
            String userName =
                    flinkConfig.get(
                            org.apache.flink.configuration.PipelineOptions.ROLE_SESSION_NAME);
            if (StringUtils.isNullOrWhitespaceOnly(userName)) {
                throw new IllegalArgumentException("role-session-name must not be null or empty");
            } else {
                catalogOptions.set("dlf.user.name", userName);
                catalogOptions.set(
                        "dlf.tokenCache.meta.credential.provider",
                        flinkConfig.get(DlfOptions.DLF_META_CREDENTIAL_PROVIDER));
                catalogOptions.set(
                        "dlf.tokenCache.meta.credential.provider.url",
                        "secrets://"
                                + flinkConfig.get(DlfOptions.DLF_META_CREDENTIAL_PROVIDER_PATH));
                catalogOptions.set(
                        "dlf.tokenCache.data.credential.provider",
                        flinkConfig.get(DlfOptions.DLF_DATA_CREDENTIAL_PROVIDER));
                catalogOptions.set(
                        "dlf.tokenCache.data.credential.provider.url",
                        "secrets://"
                                + flinkConfig.get(DlfOptions.DLF_DATA_CREDENTIAL_PROVIDER_PATH));
                catalogOptions.set("fs.dlf.impl.disable.cache", "true");
                LOGGER.debug("DlfPaimon catalog options: {}", catalogOptions.toMap());
            }
        }
    }
}
