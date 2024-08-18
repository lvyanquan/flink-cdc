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

package org.apache.flink.cdc.connectors.hologres.sink.v2.config;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy;
import org.apache.flink.cdc.connectors.hologres.utils.JDBCUtils;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.client.model.WriteMode;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** Builder to construct {@link HologresConnectionParam}. */
@PublicEvolving
public class HologresConnectionParamBuilder {

    // common config
    private String endpoint;
    private String database;

    private String username;

    private String password;

    private SSLMode sslMode = SSLMode.DISABLE;

    private String sslRootCertLocation;

    private Integer jdbcRetryCount = 10;
    private Long jdbcRetrySleepInitMs = 1000L;

    private Long jdbcRetrySleepStepMs = 5000L;

    private Long jdbcConnectionMaxIdleMs = 60000L;

    private Long jdbcMetaCacheTTL = 60000L;

    private Integer jdbcMetaAutoRefreshFactor = 4;

    // write mode
    private DeleteStrategy deleteStrategy = DeleteStrategy.DELETE_ROW_ON_PK;

    private WriteMode writeMode = WriteMode.INSERT_OR_UPDATE;

    private Boolean createPartitionTable = false;

    private Integer connectionPoolSize = 3;

    private Integer jdbcWriteBatchSize = 256;

    private Long jdbcWriteBatchByteSize = 2097152L;

    private Long jdbcWriteFlushInterval = 10000L;

    private Boolean jdbcEnableDefaultForNotNullColumn = true;

    private Boolean removeU0000InText = false;

    private Boolean ignoreNullWhenUpdate = false;

    private Boolean enableDeduplication = true;

    private TypeNormalizationStrategy typeNormalizationStrategy = TypeNormalizationStrategy.NORMAL;

    private String jdbcSharedConnectionPoolName = "cdc-default";

    private boolean useFixedFe = false;

    private ZoneId zoneId = ZoneId.systemDefault();

    private Map<String, String> tableOptions = new HashMap<>();

    public HologresConnectionParamBuilder setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public HologresConnectionParamBuilder setDatabase(String database) {
        this.database = database;
        return this;
    }

    public HologresConnectionParamBuilder setUsername(String username) {
        this.username = username;
        return this;
    }

    public HologresConnectionParamBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public HologresConnectionParamBuilder setSslMode(SSLMode sslMode) {
        this.sslMode = sslMode;
        return this;
    }

    public HologresConnectionParamBuilder setSslRootCertLocation(String sslRootCertLocation) {
        this.sslRootCertLocation = sslRootCertLocation;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcRetryCount(Integer jdbcRetryCount) {
        this.jdbcRetryCount = jdbcRetryCount;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcRetrySleepInitMs(Long jdbcRetrySleepInitMs) {
        this.jdbcRetrySleepInitMs = jdbcRetrySleepInitMs;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcRetrySleepStepMs(Long jdbcRetrySleepStepMs) {
        this.jdbcRetrySleepStepMs = jdbcRetrySleepStepMs;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcConnectionMaxIdleMs(Long jdbcConnectionMaxIdleMs) {
        this.jdbcConnectionMaxIdleMs = jdbcConnectionMaxIdleMs;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcMetaCacheTTL(Long jdbcMetaCacheTTL) {
        this.jdbcMetaCacheTTL = jdbcMetaCacheTTL;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcMetaAutoRefreshFactor(
            Integer jdbcMetaAutoRefreshFactor) {
        this.jdbcMetaAutoRefreshFactor = jdbcMetaAutoRefreshFactor;
        return this;
    }

    public HologresConnectionParamBuilder setWriteMode(WriteMode writeMode) {
        this.writeMode = writeMode;
        return this;
    }

    public HologresConnectionParamBuilder setCreatePartitionTable(Boolean createPartitionTable) {
        this.createPartitionTable = createPartitionTable;
        return this;
    }

    public HologresConnectionParamBuilder setConnectionPoolSize(Integer connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcWriteBatchSize(Integer jdbcWriteBatchSize) {
        this.jdbcWriteBatchSize = jdbcWriteBatchSize;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcWriteBatchByteSize(Long jdbcWriteBatchByteSize) {
        this.jdbcWriteBatchByteSize = jdbcWriteBatchByteSize;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcWriteFlushInterval(Long jdbcWriteFlushInterval) {
        this.jdbcWriteFlushInterval = jdbcWriteFlushInterval;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcEnableDefaultForNotNullColumn(
            Boolean jdbcEnableDefaultForNotNullColumn) {
        this.jdbcEnableDefaultForNotNullColumn = jdbcEnableDefaultForNotNullColumn;
        return this;
    }

    public HologresConnectionParamBuilder setRemoveU0000InText(Boolean removeU0000InText) {
        this.removeU0000InText = removeU0000InText;
        return this;
    }

    public HologresConnectionParamBuilder setIgnoreNullWhenUpdate(Boolean ignoreNullWhenUpdate) {
        this.ignoreNullWhenUpdate = ignoreNullWhenUpdate;
        return this;
    }

    public HologresConnectionParamBuilder setEnableDeduplication(Boolean enableDeduplication) {
        this.enableDeduplication = enableDeduplication;
        return this;
    }

    public HologresConnectionParamBuilder setJdbcSharedConnectionPoolName(
            String jdbcSharedConnectionPoolName) {
        this.jdbcSharedConnectionPoolName = jdbcSharedConnectionPoolName;
        return this;
    }

    public HologresConnectionParamBuilder setTableOptions(Map<String, String> tableOptions) {
        this.tableOptions = tableOptions;
        return this;
    }

    public HologresConnectionParamBuilder setUseFixedFe(boolean useFixedFe) {
        this.useFixedFe = useFixedFe;
        return this;
    }

    public HologresConnectionParamBuilder setDeleteStrategy(DeleteStrategy deleteStrategy) {
        this.deleteStrategy = deleteStrategy;
        return this;
    }

    public HologresConnectionParamBuilder setTypeNormalizationStrategy(
            TypeNormalizationStrategy typeNormalizationStrategy) {
        this.typeNormalizationStrategy = typeNormalizationStrategy;
        return this;
    }

    public HologresConnectionParamBuilder setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
        return this;
    }

    private void sanityCheck() {
        assert endpoint != null && database != null && username != null && password != null
                : "endpoint, database, username and password are not allowed to be Null.";
    }

    @VisibleForTesting
    HoloConfig generateHoloConfig() {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(JDBCUtils.getDbUrl(endpoint, database));
        holoConfig.setAppName("ververica-connector-hologres");
        // clean up the holo-client resources when the jvm is closed to prevent the user from
        // forgetting to close it, which is not required in the connector
        holoConfig.setEnableShutdownHook(false);
        holoConfig.setUsername(username);
        holoConfig.setPassword(password);
        holoConfig.setUseFixedFe(useFixedFe);
        // For holo instance that can be directly connected, the default is direct connection,
        // unless set enable_direct_connect false.
        holoConfig.setEnableDirectConnection(false);
        // In psql, the execution of insert/update/delete statements by default will return the
        // number of affected rows. For example, inserting into an empty table with the command
        // 'insert into table values (?, ?)' would return 'INSERT 0 1'. This behavior may incur
        // additional overhead in certain scenarios; In the current implementation of holoclient,
        // there is no need to collect the number of affected rows. Considering the impact on
        // performance, affected rows collection is turned off by default.
        holoConfig.setEnableAffectedRows(true);

        // connection config
        holoConfig.setRetryCount(jdbcRetryCount);
        holoConfig.setRetrySleepInitMs(jdbcRetrySleepInitMs);
        holoConfig.setRetrySleepStepMs(jdbcRetrySleepStepMs);
        holoConfig.setConnectionMaxIdleMs(jdbcConnectionMaxIdleMs);
        holoConfig.setMetaCacheTTL(jdbcMetaCacheTTL);
        holoConfig.setMetaAutoRefreshFactor(jdbcMetaAutoRefreshFactor);
        holoConfig.setSslMode(sslMode);
        holoConfig.setSslRootCertLocation(sslRootCertLocation);

        // writer config
        holoConfig.setWriteThreadSize(connectionPoolSize);
        holoConfig.setWriteBatchSize(jdbcWriteBatchSize);
        holoConfig.setWriteBatchByteSize(jdbcWriteBatchByteSize);
        holoConfig.setWriteMaxIntervalMs(jdbcWriteFlushInterval);
        holoConfig.setEnableDefaultForNotNullColumn(jdbcEnableDefaultForNotNullColumn);
        holoConfig.setDynamicPartition(createPartitionTable);
        holoConfig.setWriteMode(writeMode);
        holoConfig.setRemoveU0000InTextColumnValue(removeU0000InText);
        holoConfig.setEnableDeduplication(enableDeduplication);
        return holoConfig;
    }

    public HologresConnectionParam build() {
        sanityCheck();
        return new HologresConnectionParam(
                endpoint,
                database,
                username,
                password,
                jdbcSharedConnectionPoolName,
                ignoreNullWhenUpdate,
                deleteStrategy,
                typeNormalizationStrategy,
                zoneId,
                tableOptions,
                generateHoloConfig());
    }
}
