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
import org.apache.flink.table.gateway.client.VvrSqlGatewayClient;
import org.apache.flink.table.gateway.client.api.SqlGatewayClient;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
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

    @BeforeAll
    static void start(@TempDir File flinkHome) throws Exception {
        SqlGatewayRestEndpoint sqlGatewayRestEndpoint =
                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getSqlGatewayRestEndpoint();
        InetSocketAddress serverAddress = checkNotNull(sqlGatewayRestEndpoint.getServerAddress());
        targetAddress = serverAddress.getHostName();
        port = serverAddress.getPort();

        okHttpClient =
                new OkHttpClient.Builder()
                        .readTimeout(Duration.ofSeconds(60))
                        .writeTimeout(Duration.ofSeconds(60))
                        .build();
        client = new VvrSqlGatewayClient(targetAddress, port, okHttpClient);

        originalEnv = System.getenv();
        // prepare yaml
        File confYaml = new File(flinkHome, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }
        // adjust the test environment for the purposes of this test
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, flinkHome.getAbsolutePath());
        CommonTestUtils.setEnv(map);
    }

    @AfterAll
    static void stop() {
        okHttpClient.dispatcher().executorService().shutdown();
        okHttpClient.connectionPool().evictAll();
        CommonTestUtils.setEnv(originalEnv);
    }
}
