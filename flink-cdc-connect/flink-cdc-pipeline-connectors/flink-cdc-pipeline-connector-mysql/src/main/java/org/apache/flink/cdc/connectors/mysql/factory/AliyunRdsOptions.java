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

package org.apache.flink.cdc.connectors.mysql.factory;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** RDS related options. */
@PublicEvolving
public class AliyunRdsOptions {

    @Experimental
    public static final ConfigOption<String> RDS_REGION_ID =
            ConfigOptions.key("rds.region-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Region ID of RDS instance");

    @Experimental
    public static final ConfigOption<String> RDS_ACCESS_KEY_ID =
            ConfigOptions.key("rds.access-key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Access key ID of RDS instance");

    @Experimental
    public static final ConfigOption<String> RDS_ACCESS_KEY_SECRET =
            ConfigOptions.key("rds.access-key-secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Access key secret of RDS instance");

    @Experimental
    public static final ConfigOption<String> RDS_DB_INSTANCE_ID =
            ConfigOptions.key("rds.db-instance-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RDS Database instance ID");

    @Experimental
    public static final ConfigOption<Duration> RDS_DOWNLOAD_TIMEOUT =
            ConfigOptions.key("rds.download.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Timeout of downloading binlog files from RDS archive");

    @Experimental
    public static final ConfigOption<String> RDS_BINLOG_DIRECTORIES_PARENT_PATH =
            ConfigOptions.key("rds.binlog-directories-parent-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute path of parent directory holding sub-directories for downloaded binlog files");

    @Experimental
    public static final ConfigOption<String> RDS_BINLOG_DIRECTORY_PREFIX =
            ConfigOptions.key("rds.binlog-directory-prefix")
                    .stringType()
                    .defaultValue("rds-binlog-")
                    .withDescription("Prefix of directory holding binlog files");

    @Experimental
    public static final ConfigOption<Boolean> RDS_USE_INTRANET_LINK =
            ConfigOptions.key("rds.use-intranet-link")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use intranet download link for downloading binlog files");

    public static AliyunRdsConfig fromConfig(Configuration other) {
        sanityCheck(other);

        Map<String, String> factoryOptions = other.toMap();
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RDS_REGION_ID.key(), factoryOptions.get(RDS_REGION_ID.key()));
        configuration.put(RDS_ACCESS_KEY_ID.key(), factoryOptions.get(RDS_ACCESS_KEY_ID.key()));
        configuration.put(
                RDS_ACCESS_KEY_SECRET.key(), factoryOptions.get(RDS_ACCESS_KEY_SECRET.key()));
        configuration.put(RDS_DB_INSTANCE_ID.key(), factoryOptions.get(RDS_DB_INSTANCE_ID.key()));
        configuration.put(
                RDS_DOWNLOAD_TIMEOUT.key(), factoryOptions.get(RDS_DOWNLOAD_TIMEOUT.key()));
        if (factoryOptions.get(RDS_BINLOG_DIRECTORIES_PARENT_PATH.key()) != null) {
            configuration.put(
                    RDS_BINLOG_DIRECTORIES_PARENT_PATH.key(),
                    factoryOptions.get(RDS_BINLOG_DIRECTORIES_PARENT_PATH.key()));
        }
        configuration.put(
                RDS_BINLOG_DIRECTORY_PREFIX.key(),
                factoryOptions.get(RDS_BINLOG_DIRECTORY_PREFIX.key()));
        configuration.put(
                RDS_USE_INTRANET_LINK.key(), factoryOptions.get(RDS_USE_INTRANET_LINK.key()));
        return new AliyunRdsConfig(
                org.apache.flink.configuration.Configuration.fromMap(configuration));
    }

    private static void sanityCheck(Configuration configuration) {
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
}
