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

package org.apache.flink.cdc.connectors.hologres.factory;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption;
import org.apache.flink.cdc.connectors.hologres.sink.HologresDataSink;

import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.client.model.WriteMode;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.DATABASE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.ENDPOINT;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_CLIENT_CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_CONNECTION_SSL_MODE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_META_CACHE_TTL;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_RETRY_COUNT;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_RETRY_SLEEP_STEP_MS;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.PASSWORD;
import static org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption.USERNAME;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.CREATE_MISSING_PARTITION_TABLE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.ENABLE_TYPE_NORMALIZATION;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.IGNORE_NULL_WHEN_UPDATE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.MUTATE_TYPE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_ENABLE_DEDUPLICATION;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_JDBC_WRITE_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.SINK_DELETE_STRATEGY;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.USE_FIXED_FE;

/** A {@link DataSinkFactory} to create {@link HologresDataSink}. */
@PublicEvolving
public class HologresDataSinkFactory implements DataSinkFactory {
    public static final String IDENTIFIER = "hologres";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(HologresDataSinkOption.TABLE_PROPERTY_PREFIX);

        Configuration configuration = context.getFactoryConfiguration();
        Map<String, String> allOptions = configuration.toMap();
        Map<String, String> tableOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(HologresDataSinkOption.TABLE_PROPERTY_PREFIX)) {
                        tableOptions.put(
                                key.substring(HologresDataSinkOption.TABLE_PROPERTY_PREFIX.length())
                                        .toLowerCase(),
                                value.toLowerCase());
                    }
                });
        String endpoint = configuration.get(ENDPOINT);
        String database = configuration.get(DATABASE);
        String username = configuration.get(USERNAME);
        String password = configuration.get(PASSWORD);
        SSLMode sslMode = configuration.get(OPTIONAL_CONNECTION_SSL_MODE);
        String sslRootCertLocation = configuration.get(OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION);
        Integer jdbcRetryCount = configuration.get(OPTIONAL_JDBC_RETRY_COUNT);
        Long jdbcRetrySleepInitMs = configuration.get(OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS);
        Long jdbcRetrySleepStepMs = configuration.get(OPTIONAL_JDBC_RETRY_SLEEP_STEP_MS);
        Long jdbcConnectionMaxIdleMs = configuration.get(OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS);
        Long jdbcMetaCacheTTL = configuration.get(OPTIONAL_JDBC_META_CACHE_TTL);
        Integer jdbcMetaAutoRefreshFactor =
                configuration.get(OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR);
        Integer connectionPoolSize = configuration.get(OPTIONAL_CLIENT_CONNECTION_POOL_SIZE);
        String jdbcShardConnectionPoolName =
                configuration.get(OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME);

        WriteMode writeMode = configuration.get(MUTATE_TYPE);
        DeleteStrategy deleteStrategy = configuration.get(SINK_DELETE_STRATEGY);
        Boolean enableTypeNormalization = configuration.get(ENABLE_TYPE_NORMALIZATION);

        Boolean ignoreNullWhenUpdate = configuration.get(IGNORE_NULL_WHEN_UPDATE);
        Boolean createPartitionTable = configuration.get(CREATE_MISSING_PARTITION_TABLE);
        Integer jdbcWriteBatchSize = configuration.get(OPTIONAL_JDBC_WRITE_BATCH_SIZE);
        Long jdbcWriteBatchByteSize = configuration.get(OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE);
        Long jdbcWriteFlushInterval = configuration.get(OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL);
        Boolean jdbcEnableDefaultForNotNullColumn =
                configuration.get(OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN);
        Boolean removeU0000InText = configuration.get(OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT);
        Boolean enableDeduplication = configuration.get(OPTIONAL_ENABLE_DEDUPLICATION);
        Boolean useFixedFe = configuration.get(USE_FIXED_FE);

        return new HologresDataSink(
                endpoint,
                database,
                username,
                password,
                sslMode,
                sslRootCertLocation,
                jdbcRetryCount,
                jdbcRetrySleepInitMs,
                jdbcRetrySleepStepMs,
                jdbcConnectionMaxIdleMs,
                jdbcMetaCacheTTL,
                jdbcMetaAutoRefreshFactor,
                writeMode,
                createPartitionTable,
                deleteStrategy,
                enableTypeNormalization,
                connectionPoolSize,
                jdbcWriteBatchSize,
                jdbcWriteBatchByteSize,
                jdbcWriteFlushInterval,
                jdbcEnableDefaultForNotNullColumn,
                removeU0000InText,
                ignoreNullWhenUpdate,
                enableDeduplication,
                jdbcShardConnectionPoolName,
                useFixedFe,
                tableOptions);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        // common
        options.add(OPTIONAL_CONNECTION_SSL_MODE);
        options.add(OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION);
        options.add(OPTIONAL_JDBC_RETRY_COUNT);
        options.add(OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS);
        options.add(OPTIONAL_JDBC_RETRY_SLEEP_STEP_MS);
        options.add(OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS);
        options.add(OPTIONAL_JDBC_META_CACHE_TTL);
        options.add(OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR);
        options.add(OPTIONAL_CLIENT_CONNECTION_POOL_SIZE);
        options.add(OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME);

        // sink only
        options.add(MUTATE_TYPE);
        options.add(USE_FIXED_FE);
        options.add(IGNORE_NULL_WHEN_UPDATE);
        options.add(CREATE_MISSING_PARTITION_TABLE);
        options.add(OPTIONAL_JDBC_WRITE_BATCH_SIZE);
        options.add(OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE);
        options.add(OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE);
        options.add(OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL);
        options.add(OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN);
        options.add(OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT);
        options.add(ENABLE_TYPE_NORMALIZATION);
        options.add(SINK_DELETE_STRATEGY);
        return options;
    }
}

// to
