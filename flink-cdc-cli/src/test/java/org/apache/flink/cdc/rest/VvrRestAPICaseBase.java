/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.rest;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.catalog.FileCatalogStoreFactoryOptions;
import org.apache.flink.table.gateway.client.api.SqlGatewayClient;
import org.apache.flink.table.gateway.client.api.config.CommonSqlGatewayClientConfigOptions;
import org.apache.flink.table.gateway.client.api.utils.SqlGatewayClientUtils;
import org.apache.flink.table.gateway.rest.SqlGatewayRestEndpoint;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.MATERIALIZED_TABLE_EXEC_INFER_SOURCE_PARALLELISM_ENABLED;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_CATALOG_NAME;
import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_KIND;
import static org.apache.flink.table.catalog.CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class for VVR Rest API IT test. Copy from
 * org.apache.flink.table.gateway.vvr.rest.VvrRestAPICaseBase, but construct SqlGatewayClient
 * directly rather than load by class loader.
 */
public class VvrRestAPICaseBase {

    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    protected static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    @RegisterExtension
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    () ->
                            Executors.newCachedThreadPool(
                                    new ExecutorThreadFactory(
                                            "VVR Test Pool", IgnoreExceptionHandler.INSTANCE)));

    @Nullable protected static String targetAddress = null;
    private static Map<String, String> originalEnv;

    protected static int port = 0;
    protected static OkHttpClient okHttpClient;
    protected static SqlGatewayClient client;

    @TempDir protected static Path fileCatalogStorePath;
    protected static final String VALIDATOR_CATALOG_NAME = "validatorCat";

    @BeforeAll
    static void start(@TempDir File flinkHome) throws Exception {
        SqlGatewayRestEndpoint sqlGatewayRestEndpoint =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getSqlGatewayRestEndpoint();
        InetSocketAddress serverAddress = checkNotNull(sqlGatewayRestEndpoint.getServerAddress());
        targetAddress = serverAddress.getHostName();
        port = serverAddress.getPort();

        Map<String, String> map = new HashMap<>();
        map.put(CommonSqlGatewayClientConfigOptions.SQL_GATEWAY_CLIENT_IDENTIFIER.key(), "vvr");
        map.put(
                CommonSqlGatewayClientConfigOptions.SQL_GATEWAY_CLIENT_REST_ADDRESS.key(),
                targetAddress);
        map.put(
                CommonSqlGatewayClientConfigOptions.SQL_GATEWAY_CLIENT_REST_PORT.key(),
                String.valueOf(port));
        okHttpClient = new OkHttpClient();
        client =
                SqlGatewayClientUtils.createClient(
                        map, VvrRestAPICaseBase.class.getClassLoader(), okHttpClient);

        originalEnv = System.getenv();
        // prepare yaml
        File confYaml = new File(flinkHome, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }
        // adjust the test environment for the purposes of this test
        Map<String, String> map2 = new HashMap<>(System.getenv());
        map2.put(ENV_FLINK_CONF_DIR, flinkHome.getAbsolutePath());
        CommonTestUtils.setEnv(map2);
    }

    @AfterAll
    static void stop() {
        okHttpClient.dispatcher().executorService().shutdown();
        okHttpClient.connectionPool().evictAll();
        CommonTestUtils.setEnv(originalEnv);
    }

    protected static Map<String, String> getFileCatalogStoreConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(TABLE_CATALOG_STORE_KIND.key(), FileCatalogStoreFactoryOptions.IDENTIFIER);
        config.put(
                TABLE_CATALOG_STORE_OPTION_PREFIX
                        + FileCatalogStoreFactoryOptions.IDENTIFIER
                        + "."
                        + FileCatalogStoreFactoryOptions.PATH.key(),
                fileCatalogStorePath.toUri().toString());
        config.put(TABLE_CATALOG_NAME.key(), VALIDATOR_CATALOG_NAME);
        config.put(MATERIALIZED_TABLE_EXEC_INFER_SOURCE_PARALLELISM_ENABLED.key(), "true");
        return config;
    }
}
