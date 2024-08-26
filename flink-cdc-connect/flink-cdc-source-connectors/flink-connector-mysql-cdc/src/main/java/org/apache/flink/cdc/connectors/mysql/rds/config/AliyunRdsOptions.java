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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

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

    @Experimental
    public static final ConfigOption<String> RDS_MAIN_DB_ID =
            ConfigOptions.key("rds.main-db-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The identifier of main database instance");

    @Experimental
    public static final ConfigOption<String> RDS_BINLOG_ENDPOINT =
            ConfigOptions.key("rds.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("RDS Endpoint.");
}
