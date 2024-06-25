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

package org.apache.flink.cdc.connectors.hologres.config;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import com.alibaba.hologres.client.model.SSLMode;

/** Common configs used for source and sink. */
public class HologresCommonOption {

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Endpoint of hologres instance.");
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("dbname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of hologres instance.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username of hologres database.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password of hologres database.");

    // JDBC config
    public static final ConfigOption<Integer> OPTIONAL_CLIENT_CONNECTION_POOL_SIZE =
            ConfigOptions.key("connectionSize".toLowerCase())
                    .intType()
                    .defaultValue(5)
                    .withDescription("The size of the Hologres JDBC connection pool.");
    public static final ConfigOption<String> OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME =
            ConfigOptions.key("connectionPoolName".toLowerCase())
                    .stringType()
                    .defaultValue("cdc-default")
                    .withDescription(
                            "The name of the connection pool. Within the same TaskManager, tables with the same connection pool name configuration can share the connection pool.");
    public static final ConfigOption<SSLMode> OPTIONAL_CONNECTION_SSL_MODE =
            ConfigOptions.key("connection.ssl.mode".toLowerCase())
                    .enumType(SSLMode.class)
                    .defaultValue(SSLMode.DISABLE)
                    .withDescription(
                            "Whether to enable SSL (Secure Sockets Layer) transport encryption and which mode to use. The parameter values are as follows:\n"
                                    + "\n"
                                    + "disable (default): Transport encryption is not enabled.\n"
                                    + "require: Enable SSL for encryption of the data link only.\n"
                                    + "verify-ca: Enable SSL, encrypt the data link, and use a CA certificate to verify the authenticity of the Hologres server.\n"
                                    + "verify-full: Enable SSL, encrypt the data link, use a CA certificate to verify the authenticity of the Hologres server, and also compare whether the CN or DNS in the certificate matches the Hologres connection address configured at the time of connection.");
    public static final ConfigOption<String> OPTIONAL_CONNECTION_SSL_ROOT_CERT_LOCATION =
            ConfigOptions.key("connection.ssl.root-cert.location".toLowerCase())
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Users need to upload files to vvp in advance, and the path must be /flink/usrlib/${certificate file name}");

    public static final ConfigOption<Integer> OPTIONAL_JDBC_RETRY_COUNT =
            ConfigOptions.key("jdbcRetryCount".toLowerCase())
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The number of retries for writing and querying when there is a connection failure. The default value is 10.");
    public static final ConfigOption<Long> OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS =
            ConfigOptions.key("jdbcRetrySleepInitMs".toLowerCase())
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "The fixed waiting time for each retry. The actual waiting time for retries is calculated using the formula jdbcRetrySleepInitMs + retry * jdbcRetrySleepStepMs. The unit is milliseconds.");
    public static final ConfigOption<Long> OPTIONAL_JDBC_RETRY_SLEEP_STEP_MS =
            ConfigOptions.key("jdbcRetrySleepStepMs".toLowerCase())
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "The cumulative waiting time for each retry. The actual waiting time for retries is calculated using the formula jdbcRetrySleepInitMs + retry * jdbcRetrySleepStepMs. The unit is milliseconds.");
    public static final ConfigOption<Long> OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS =
            ConfigOptions.key("jdbcConnectionMaxIdleMs".toLowerCase())
                    .longType()
                    .defaultValue(60000L)
                    .withDescription(
                            "The idle time of a JDBC connection. If this idle time is exceeded, the connection will be disconnected and released. The unit is in milliseconds.");
    public static final ConfigOption<Long> OPTIONAL_JDBC_META_CACHE_TTL =
            ConfigOptions.key("jdbcMetaCacheTTL".toLowerCase())
                    .longType()
                    .defaultValue(60000L)
                    .withDescription(
                            "The expiration time for locally cached TableSchema information. The unit is milliseconds.");

    public static final ConfigOption<Integer> OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR =
            ConfigOptions.key("jdbcMetaAutoRefreshFactor".toLowerCase())
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "If the remaining time of the cache is less than the trigger time, the system will automatically refresh the cache.");
}
