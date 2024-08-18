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

package org.apache.flink.cdc.connectors.hologres.sink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy;
import org.apache.flink.cdc.connectors.hologres.sink.v2.HologresRecordSerializer;
import org.apache.flink.cdc.connectors.hologres.sink.v2.HologresSink;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParamBuilder;

import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.client.model.WriteMode;
import com.esotericsoftware.minlog.Log;

import java.time.ZoneId;
import java.util.Map;

/** A {@link DataSink} for Hologres connector that supports schema evolution. */
@Internal
public class HologresDataSink implements DataSink {
    private final String endpoint;
    private final String database;

    private final String username;

    private final String password;

    private final SSLMode sslMode;

    private final String sslRootCertLocation;

    private final Integer jdbcRetryCount;
    private final Long jdbcRetrySleepInitMs;

    private final Long jdbcRetrySleepStepMs;

    private final Long jdbcConnectionMaxIdleMs;

    private final Long jdbcMetaCacheTTL;

    private final Integer jdbcMetaAutoRefreshFactor;

    private final WriteMode writeMode;

    private final Boolean createPartitionTable;

    private final DeleteStrategy deleteStrategy;

    private final Integer connectionPoolSize;

    private final Integer jdbcWriteBatchSize;

    private final Long jdbcWriteBatchByteSize;

    private final Long jdbcWriteFlushInterval;

    private final Boolean jdbcEnableDefaultForNotNullColumn;

    private final Boolean removeU0000InText;

    private final Boolean ignoreNullWhenUpdate;

    private final Boolean enableDeduplication;

    private final String jdbcSharedConnectionPoolName;

    private final Map<String, String> tableOptions;

    private final boolean useFixedFe;

    private final TypeNormalizationStrategy typeNormalizationStrategy;

    private final ZoneId zoneId;

    public HologresDataSink(
            String endpoint,
            String database,
            String username,
            String password,
            SSLMode sslMode,
            String sslRootCertLocation,
            Integer jdbcRetryCount,
            Long jdbcRetrySleepInitMs,
            Long jdbcRetrySleepStepMs,
            Long jdbcConnectionMaxIdleMs,
            Long jdbcMetaCacheTTL,
            Integer jdbcMetaAutoRefreshFactor,
            WriteMode writeMode,
            Boolean createPartitionTable,
            DeleteStrategy deleteStrategy,
            TypeNormalizationStrategy typeNormalizationStrategy,
            Integer connectionPoolSize,
            Integer jdbcWriteBatchSize,
            Long jdbcWriteBatchByteSize,
            Long jdbcWriteFlushInterval,
            Boolean jdbcEnableDefaultForNotNullColumn,
            Boolean removeU0000InText,
            Boolean ignoreNullWhenUpdate,
            Boolean enableDeduplication,
            String jdbcSharedConnectionPoolName,
            Boolean useFixedFe,
            ZoneId zoneId,
            Map<String, String> tableOptions) {
        this.endpoint = endpoint;
        this.database = database;
        this.username = username;
        this.password = password;
        this.sslMode = sslMode;
        this.sslRootCertLocation = sslRootCertLocation;
        this.jdbcRetryCount = jdbcRetryCount;
        this.jdbcRetrySleepInitMs = jdbcRetrySleepInitMs;
        this.jdbcRetrySleepStepMs = jdbcRetrySleepStepMs;
        this.jdbcConnectionMaxIdleMs = jdbcConnectionMaxIdleMs;
        this.jdbcMetaCacheTTL = jdbcMetaCacheTTL;
        this.jdbcMetaAutoRefreshFactor = jdbcMetaAutoRefreshFactor;
        this.writeMode = writeMode;
        this.createPartitionTable = createPartitionTable;
        this.deleteStrategy = deleteStrategy;
        this.typeNormalizationStrategy = typeNormalizationStrategy;
        this.connectionPoolSize = connectionPoolSize;
        this.jdbcWriteBatchSize = jdbcWriteBatchSize;
        this.jdbcWriteBatchByteSize = jdbcWriteBatchByteSize;
        this.jdbcWriteFlushInterval = jdbcWriteFlushInterval;
        this.jdbcEnableDefaultForNotNullColumn = jdbcEnableDefaultForNotNullColumn;
        this.removeU0000InText = removeU0000InText;
        this.ignoreNullWhenUpdate = ignoreNullWhenUpdate;
        this.enableDeduplication = enableDeduplication;
        this.jdbcSharedConnectionPoolName = jdbcSharedConnectionPoolName;
        this.tableOptions = tableOptions;
        this.useFixedFe = useFixedFe;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        HologresConnectionParamBuilder hologresConnectionParamBuilder =
                HologresConnectionParam.builder();
        HologresConnectionParam hologresConnectionParam =
                hologresConnectionParamBuilder
                        .setEndpoint(endpoint)
                        .setDatabase(database)
                        .setUsername(username)
                        .setPassword(password)
                        .setSslMode(sslMode)
                        .setSslRootCertLocation(sslRootCertLocation)
                        .setJdbcRetryCount(jdbcRetryCount)
                        .setJdbcRetrySleepInitMs(jdbcRetrySleepInitMs)
                        .setJdbcRetrySleepStepMs(jdbcRetrySleepStepMs)
                        .setJdbcConnectionMaxIdleMs(jdbcConnectionMaxIdleMs)
                        .setJdbcMetaCacheTTL(jdbcMetaCacheTTL)
                        .setJdbcMetaAutoRefreshFactor(jdbcMetaAutoRefreshFactor)
                        .setWriteMode(writeMode)
                        .setCreatePartitionTable(createPartitionTable)
                        .setDeleteStrategy(deleteStrategy)
                        .setTypeNormalizationStrategy(typeNormalizationStrategy)
                        .setConnectionPoolSize(connectionPoolSize)
                        .setJdbcWriteBatchSize(jdbcWriteBatchSize)
                        .setJdbcWriteBatchByteSize(jdbcWriteBatchByteSize)
                        .setJdbcWriteFlushInterval(jdbcWriteFlushInterval)
                        .setJdbcEnableDefaultForNotNullColumn(jdbcEnableDefaultForNotNullColumn)
                        .setRemoveU0000InText(removeU0000InText)
                        .setIgnoreNullWhenUpdate(ignoreNullWhenUpdate)
                        .setJdbcSharedConnectionPoolName(jdbcSharedConnectionPoolName)
                        .setEnableDeduplication(enableDeduplication)
                        .setUseFixedFe(useFixedFe)
                        .setTableOptions(tableOptions)
                        .setZoneId(zoneId)
                        .build();
        Log.info("param is: " + hologresConnectionParam);
        HologresRecordSerializer serializer =
                new HologresRecordEventSerializer(hologresConnectionParam);
        return FlinkSinkProvider.of(new HologresSink<Event>(hologresConnectionParam, serializer));
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        HologresConnectionParamBuilder hologresConnectionParamBuilder =
                HologresConnectionParam.builder();
        HologresConnectionParam hologresConnectionParam =
                hologresConnectionParamBuilder
                        .setEndpoint(endpoint)
                        .setDatabase(database)
                        .setUsername(username)
                        .setPassword(password)
                        .setSslMode(sslMode)
                        .setSslRootCertLocation(sslRootCertLocation)
                        .setTableOptions(tableOptions)
                        .build();
        return new HologresMetadataApplier(hologresConnectionParam);
    }
}
