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

package org.apache.flink.cdc.connectors.mysql.rds.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_ACCESS_KEY_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_ACCESS_KEY_SECRET;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_BINLOG_DIRECTORIES_PARENT_PATH;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_BINLOG_DIRECTORY_PREFIX;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_BINLOG_ENDPOINT;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_DB_INSTANCE_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_DOWNLOAD_TIMEOUT;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_MAIN_DB_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_REGION_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_USE_INTRANET_LINK;
import static org.apache.flink.util.Preconditions.checkState;

/** RDS related configurations. */
@PublicEvolving
public class AliyunRdsConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Configuration configuration;

    /** Create a new {@link RdsConfigBuilder} instance. */
    public static RdsConfigBuilder builder() {
        return new RdsConfigBuilder();
    }

    /** Load RDS related configurations from an existing configuration. */
    public static AliyunRdsConfig fromConfig(ReadableConfig other) {
        sanityCheck(other);
        Configuration configuration = new Configuration();
        configuration.set(RDS_REGION_ID, other.get(RDS_REGION_ID));
        configuration.set(RDS_ACCESS_KEY_ID, other.get(RDS_ACCESS_KEY_ID));
        configuration.set(RDS_ACCESS_KEY_SECRET, other.get(RDS_ACCESS_KEY_SECRET));
        configuration.set(RDS_DB_INSTANCE_ID, other.get(RDS_DB_INSTANCE_ID));
        configuration.set(RDS_DOWNLOAD_TIMEOUT, other.get(RDS_DOWNLOAD_TIMEOUT));
        if (other.getOptional(RDS_BINLOG_DIRECTORIES_PARENT_PATH).isPresent()) {
            configuration.set(
                    RDS_BINLOG_DIRECTORIES_PARENT_PATH,
                    other.get(RDS_BINLOG_DIRECTORIES_PARENT_PATH));
        }
        configuration.set(RDS_BINLOG_DIRECTORY_PREFIX, other.get(RDS_BINLOG_DIRECTORY_PREFIX));
        configuration.set(RDS_USE_INTRANET_LINK, other.get(RDS_USE_INTRANET_LINK));
        if (other.getOptional(RDS_MAIN_DB_ID).isPresent()) {
            configuration.set(RDS_MAIN_DB_ID, other.get(RDS_MAIN_DB_ID));
        }
        if (other.getOptional(RDS_BINLOG_ENDPOINT).isPresent()) {
            configuration.set(RDS_BINLOG_ENDPOINT, other.get(RDS_BINLOG_ENDPOINT));
        }
        return new AliyunRdsConfig(configuration);
    }

    public AliyunRdsConfig(Configuration configuration) {
        this.configuration = configuration;
    }

    // --------------------------- Getters ------------------------------
    public String getRegionId() {
        return configuration.get(RDS_REGION_ID);
    }

    public String getAccessKeyId() {
        return configuration.get(RDS_ACCESS_KEY_ID);
    }

    public String getAccessKeySecret() {
        return configuration.get(RDS_ACCESS_KEY_SECRET);
    }

    public String getDbInstanceId() {
        return configuration.get(RDS_DB_INSTANCE_ID);
    }

    public Duration getDownloadTimeout() {
        return configuration.get(RDS_DOWNLOAD_TIMEOUT);
    }

    @Nullable
    public String getEndpoint() {
        return configuration.getOptional(RDS_BINLOG_ENDPOINT).orElse(null);
    }

    public Path getRandomBinlogDirectoryPath() {
        Optional<String> optionalParent =
                configuration.getOptional(RDS_BINLOG_DIRECTORIES_PARENT_PATH);
        String prefix = configuration.get(RDS_BINLOG_DIRECTORY_PREFIX);
        if (!optionalParent.isPresent()) {
            try {
                return Files.createTempDirectory(prefix);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create temporary directory", e);
            }
        }
        return Paths.get(optionalParent.get(), prefix + RandomStringUtils.randomAlphabetic(6));
    }

    public boolean isUseIntranetLink() {
        return configuration.get(RDS_USE_INTRANET_LINK);
    }

    public String getMainDbId() {
        return configuration.get(RDS_MAIN_DB_ID);
    }

    /** Builder of {@link AliyunRdsConfig}. */
    public static class RdsConfigBuilder {
        private final Configuration configuration = new Configuration();

        /** Region ID of RDS instance. */
        public RdsConfigBuilder regionId(String regionId) {
            configuration.set(RDS_REGION_ID, regionId);
            return this;
        }

        /** Access key ID for accessing RDS instance. */
        public RdsConfigBuilder accessKeyId(String accessKeyId) {
            configuration.set(RDS_ACCESS_KEY_ID, accessKeyId);
            return this;
        }

        /** Access key secret for accessing RDS instance. */
        public RdsConfigBuilder accessKeySecret(String accessKeySecret) {
            configuration.set(RDS_ACCESS_KEY_SECRET, accessKeySecret);
            return this;
        }

        /** RDS database instance ID. */
        public RdsConfigBuilder dbInstanceId(String dbInstanceId) {
            configuration.set(RDS_DB_INSTANCE_ID, dbInstanceId);
            return this;
        }

        /**
         * Timeout of downloading binlog files from RDS archive.
         *
         * <p>Note that downloading will be attempted repeatedly until succeed within the timeout.
         */
        public RdsConfigBuilder downloadTimeout(Duration downloadTimeout) {
            configuration.set(RDS_DOWNLOAD_TIMEOUT, downloadTimeout);
            return this;
        }

        /**
         * Absolute parent path for sub-directories that hold downloaded binlog files. RDS binlog
         * fetcher will downloaded binlog files into a newly created sub-directory under this path
         * in order to avoid concurrent issues across subtasks.
         */
        public RdsConfigBuilder binlogDirectoriesParentPath(String binlogDirectoriesParentPath) {
            configuration.set(RDS_BINLOG_DIRECTORIES_PARENT_PATH, binlogDirectoriesParentPath);
            return this;
        }

        /**
         * Prefix of the sub-directory holding downloaded binlog files. RDS fetcher will append a
         * random string to this prefix for creating sub-directories.
         */
        public RdsConfigBuilder binlogDirectoryPrefix(String binlogDirectoryPrefix) {
            configuration.set(RDS_BINLOG_DIRECTORY_PREFIX, binlogDirectoryPrefix);
            return this;
        }

        /**
         * Whether to use intranet link (link accessible only within VPC) to download binlog files.
         */
        public RdsConfigBuilder useIntranetLink(boolean useIntranetLink) {
            configuration.set(RDS_USE_INTRANET_LINK, useIntranetLink);
            return this;
        }

        /** The identifier of main database instance. */
        public RdsConfigBuilder mainDbId(String mainDbId) {
            configuration.set(RDS_MAIN_DB_ID, mainDbId);
            return this;
        }

        /** The endpoint of rds client. */
        public RdsConfigBuilder endpoint(String endpoint) {
            configuration.set(RDS_BINLOG_ENDPOINT, endpoint);
            return this;
        }

        /** Build {@link AliyunRdsConfig}. */
        public AliyunRdsConfig build() {
            sanityCheck(configuration);
            return new AliyunRdsConfig(configuration);
        }
    }

    private static void sanityCheck(ReadableConfig configuration) {
        checkState(
                configuration.getOptional(RDS_ACCESS_KEY_ID).isPresent(),
                "%s is required if reading archived binlog is enabled.",
                "RDS access key ID");
        checkState(
                configuration.getOptional(RDS_ACCESS_KEY_SECRET).isPresent(),
                "%s is required if reading archived binlog is enabled.",
                "RDS access key secret");
        checkState(
                configuration.getOptional(RDS_DB_INSTANCE_ID).isPresent(),
                "%s is required if reading archived binlog is enabled.",
                "RDS database instance ID");
        checkState(
                configuration.getOptional(RDS_REGION_ID).isPresent(),
                "%s is required if reading archived binlog is enabled.",
                "RDS region ID");
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AliyunRdsConfig)) {
            return false;
        }
        AliyunRdsConfig that = (AliyunRdsConfig) o;
        return Objects.equals(this.configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration);
    }
}
