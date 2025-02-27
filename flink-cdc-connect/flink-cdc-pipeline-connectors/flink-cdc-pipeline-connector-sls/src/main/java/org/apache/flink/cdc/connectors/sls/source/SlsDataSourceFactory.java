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

package org.apache.flink.cdc.connectors.sls.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.sls.source.metadata.SlsReadableMetadata;

import com.alibaba.ververica.connectors.sls.SLSAccessInfo;
import com.alibaba.ververica.connectors.sls.SLSUtils;
import com.alibaba.ververica.connectors.sls.source.StartupMode;
import com.aliyun.openservices.log.common.Consts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.inference.SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY;
import static org.apache.flink.cdc.common.utils.OptionUtils.VVR_START_TIME_MS;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.ACCESS_ID;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.ACCESS_KEY;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.BATCH_GET_SIZE;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.COMPRESS_TYPE;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.CONSUMER_GROUP;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.ENDPOINT;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.EXIT_AFTER_FINISH;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.LOGSTORE;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.MAX_RETRIES;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.PROJECT;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.QUERY;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.REGION_ID;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.SCAN_MAX_PRE_FETCH_LOG_GROUPS;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.SHARD_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.SHARD_MOD_DIVISOR;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.SHARD_MOD_REMAINDER;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.SIGN_VERSION;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.STARTUP_MODE;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.START_TIME;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.STOP_TIME;
import static org.apache.flink.cdc.connectors.sls.source.SlsOptions.TIME_ZONE;

/** A {@link DataSourceFactory} to create {@link SlsDataSource}. */
public class SlsDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "sls";

    @Override
    public DataSource createDataSource(Context context) {
        Context normalizeContext = FactoryHelper.normalizeContext(this, context);
        FactoryHelper helper = FactoryHelper.createFactoryHelper(this, normalizeContext);
        helper.validate();

        Configuration configuration = normalizeContext.getFactoryConfiguration();

        SchemaInferenceStrategy schemaInferenceStrategy =
                configuration.get(SCHEMA_INFERENCE_STRATEGY);
        int maxFetchRecords = configuration.get(SCAN_MAX_PRE_FETCH_LOG_GROUPS);
        if (schemaInferenceStrategy == SchemaInferenceStrategy.STATIC && maxFetchRecords <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be greater than 0 if schema inference strategy is static.",
                            SCAN_MAX_PRE_FETCH_LOG_GROUPS.key()));
        }

        SLSAccessInfo accessInfo = parseAccessInfo(configuration);

        Long startTimeMs = configuration.get(VVR_START_TIME_MS);
        String startDateTime = configuration.get(START_TIME);
        String stopDateTime = configuration.get(STOP_TIME);
        String timeZone = configuration.get(TIME_ZONE);

        int startInSec;
        if (startTimeMs != null && startTimeMs > 0) {
            // Overriding startup mode to TIMESTAMP
            accessInfo.setStartupMode(StartupMode.TIMESTAMP);
            startInSec = (int) (startTimeMs / 1000);
        } else {
            startInSec = SLSUtils.getStartTimeInSecs(startDateTime, timeZone);
        }
        int stopInSec = SLSUtils.getStopTimeInSecs(stopDateTime, timeZone);

        boolean exitAfterFinish = configuration.get(EXIT_AFTER_FINISH);
        long shardDiscoveryIntervalMs = configuration.get(SHARD_DISCOVERY_INTERVAL_MS);

        String metadataList = configuration.get(METADATA_LIST);
        List<SlsReadableMetadata> readableMetadataList = listReadableMetadata(metadataList);

        return new SlsDataSource(
                accessInfo,
                startInSec,
                stopInSec,
                exitAfterFinish,
                shardDiscoveryIntervalMs,
                schemaInferenceStrategy,
                maxFetchRecords,
                readableMetadataList);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(PROJECT);
        options.add(LOGSTORE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ACCESS_ID);
        options.add(ACCESS_KEY);
        options.add(SCHEMA_INFERENCE_STRATEGY);
        options.add(SCAN_MAX_PRE_FETCH_LOG_GROUPS);
        options.add(SHARD_DISCOVERY_INTERVAL_MS);
        options.add(STARTUP_MODE);
        options.add(START_TIME);
        options.add(VVR_START_TIME_MS);
        options.add(STOP_TIME);
        options.add(CONSUMER_GROUP);
        options.add(BATCH_GET_SIZE);
        options.add(MAX_RETRIES);
        options.add(EXIT_AFTER_FINISH);
        options.add(QUERY);
        options.add(COMPRESS_TYPE);
        options.add(TIME_ZONE);
        options.add(REGION_ID);
        options.add(SIGN_VERSION);
        options.add(SHARD_MOD_DIVISOR);
        options.add(SHARD_MOD_REMAINDER);
        options.add(METADATA_LIST);
        return options;
    }

    private SLSAccessInfo parseAccessInfo(Configuration configuration) {
        SLSAccessInfo accessInfo = new SLSAccessInfo();
        accessInfo.setEndpoint(configuration.get(ENDPOINT));
        accessInfo.setAccessId(configuration.get(ACCESS_ID));
        accessInfo.setAccessKey(configuration.get(ACCESS_KEY));
        accessInfo.setProjectName(configuration.get(PROJECT));
        accessInfo.setLogstore(configuration.get(LOGSTORE));
        accessInfo.setStartupMode(configuration.get(STARTUP_MODE));
        accessInfo.setBatchGetSize(configuration.get(BATCH_GET_SIZE));
        accessInfo.setConsumerGroup(configuration.get(CONSUMER_GROUP));
        accessInfo.setMaxRetries(configuration.get(MAX_RETRIES));
        accessInfo.setQuery(configuration.get(QUERY));
        accessInfo.setRegionId(configuration.get(REGION_ID));
        accessInfo.setSignVersion(configuration.get(SIGN_VERSION));
        accessInfo.setShardModDivisor(configuration.get(SHARD_MOD_DIVISOR));
        accessInfo.setShardModRemainder(configuration.get(SHARD_MOD_REMAINDER));
        String compressType = configuration.get(COMPRESS_TYPE);
        if (!StringUtils.isNullOrWhitespaceOnly(compressType)) {
            accessInfo.setCompressType(Consts.CompressType.fromString(compressType));
        }
        return accessInfo;
    }

    private List<SlsReadableMetadata> listReadableMetadata(String metadataList) {
        if (StringUtils.isNullOrWhitespaceOnly(metadataList)) {
            return Collections.emptyList();
        }
        Set<String> readableMetadataList =
                Arrays.stream(metadataList.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
        List<SlsReadableMetadata> foundMetadata = new ArrayList<>();
        for (SlsReadableMetadata metadata : SlsReadableMetadata.values()) {
            if (readableMetadataList.contains(metadata.getKey())) {
                foundMetadata.add(metadata);
                readableMetadataList.remove(metadata.getKey());
            }
        }
        if (readableMetadataList.isEmpty()) {
            return foundMetadata;
        }
        throw new IllegalArgumentException(
                String.format(
                        "[%s] cannot be found in sls metadata.",
                        String.join(", ", readableMetadataList)));
    }
}
