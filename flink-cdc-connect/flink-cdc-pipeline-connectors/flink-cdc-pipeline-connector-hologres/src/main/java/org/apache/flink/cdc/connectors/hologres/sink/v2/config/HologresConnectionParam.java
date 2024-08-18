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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy;

import com.alibaba.hologres.client.HoloConfig;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;

/** Param of hologres sink. */
@Internal
public class HologresConnectionParam implements Serializable {
    private static final long serialVersionUID = 1371382980680745051L;

    // common config
    private String endpoint;
    private String database;

    private String username;

    private String password;

    private final Boolean ignoreNullWhenUpdate;

    private final HoloConfig holoConfig;

    @Nullable private final String jdbcSharedConnectionPoolName;

    private final DeleteStrategy deleteStrategy;

    private final Map<String, String> tableOptions;

    private final TypeNormalizationStrategy typeNormalizationStrategy;

    private final ZoneId zoneId;

    public HologresConnectionParam(
            String endpoint,
            String database,
            String username,
            String password,
            @Nullable String jdbcSharedConnectionPoolName,
            Boolean ignoreNullWhenUpdate,
            DeleteStrategy deleteStrategy,
            TypeNormalizationStrategy typeNormalizationStrategy,
            ZoneId zoneId,
            Map<String, String> tableOptions,
            HoloConfig holoConfig) {
        this.endpoint = endpoint;
        this.database = database;
        this.username = username;
        this.password = password;
        this.holoConfig = holoConfig;
        this.jdbcSharedConnectionPoolName = jdbcSharedConnectionPoolName;
        this.ignoreNullWhenUpdate = ignoreNullWhenUpdate;
        this.deleteStrategy = deleteStrategy;
        this.typeNormalizationStrategy = typeNormalizationStrategy;
        this.tableOptions = tableOptions;
        this.zoneId = zoneId;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public HoloConfig getHoloConfig() {
        return holoConfig;
    }

    public Map<String, String> getTableOptions() {
        return tableOptions;
    }

    @Nullable
    public String getJdbcSharedConnectionPoolName() {
        return jdbcSharedConnectionPoolName;
    }

    public Boolean isIgnoreNullWhenUpdate() {
        return ignoreNullWhenUpdate;
    }

    public DeleteStrategy getDeleteStrategy() {
        return deleteStrategy;
    }

    public TypeNormalizationStrategy getTypeNormalizationStrategy() {
        return typeNormalizationStrategy;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    /**
     * Create a {@link HologresConnectionParamBuilder} to construct a new {@link
     * HologresConnectionParam}.
     *
     * @return {@link HologresConnectionParamBuilder}
     */
    public static HologresConnectionParamBuilder builder() {
        return new HologresConnectionParamBuilder();
    }

    @Override
    public String toString() {
        return "HologresConnectionParam{"
                + "endpoint='"
                + endpoint
                + '\''
                + ", database='"
                + database
                + '\''
                + ", username='"
                + username
                + '\''
                + ", ignoreNullWhenUpdate="
                + ignoreNullWhenUpdate
                + '\''
                + ", deleteStrategy="
                + deleteStrategy
                + '\''
                + ", typeNormalizationStrategy="
                + typeNormalizationStrategy
                + '\''
                + ", holoConfig="
                + printHoloConfig(holoConfig)
                + '\''
                + ", jdbcSharedConnectionPoolName='"
                + jdbcSharedConnectionPoolName
                + '\''
                + ", tableOptions="
                + tableOptions
                + '\''
                + ", zoneId="
                + zoneId
                + '}';
    }

    private String printHoloConfig(HoloConfig holoConfig) {
        return "HoloConfig{"
                + "useFixedFe='"
                + holoConfig.isUseFixedFe()
                + '\''
                + ", retryCount='"
                + holoConfig.getRetryCount()
                + '\''
                + ", retrySleepInitMs='"
                + holoConfig.getRetrySleepInitMs()
                + '\''
                + ", retrySleepStepMs='"
                + holoConfig.getRetrySleepStepMs()
                + '\''
                + ", connectionMaxIdleMs='"
                + holoConfig.getConnectionMaxIdleMs()
                + '\''
                + ", metaCacheTTL='"
                + holoConfig.getMetaCacheTTL()
                + '\''
                + ", metaAutoRefreshFactor='"
                + holoConfig.getMetaAutoRefreshFactor()
                + '\''
                + ", sslMode='"
                + holoConfig.getSslMode()
                + '\''
                + ", sslRootCertLocation='"
                + holoConfig.getSslRootCertLocation()
                + '\''
                + ", writeThreadSize='"
                + holoConfig.getWriteThreadSize()
                + '\''
                + ", writeBatchSize='"
                + holoConfig.getWriteBatchSize()
                + '\''
                + ", writeBatchByteSize='"
                + holoConfig.getWriteBatchByteSize()
                + '\''
                + ", writeMaxIntervalMs='"
                + holoConfig.getWriteMaxIntervalMs()
                + '\''
                + ", enableDefaultForNotNullColumn='"
                + holoConfig.isEnableDefaultForNotNullColumn()
                + '\''
                + ", dynamicPartition='"
                + holoConfig.isDynamicPartition()
                + '\''
                + ", writeMode='"
                + holoConfig.getWriteMode()
                + '\''
                + ", removeU0000InTextColumnValue='"
                + holoConfig.isRemoveU0000InTextColumnValue()
                + '\''
                + ", enableDeduplication='"
                + holoConfig.isEnableDeduplication()
                + "}";
    }
}
