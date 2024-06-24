/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.shyiko.mysql.binlog;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceTestBase;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/** Tests for {@link BinaryLogClient}. */
public class BinaryLogClientTest extends MySqlSourceTestBase {

    @Parameterized.Parameters(name = "debeziumProperties: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {new HashMap<>()}};
    }

    @Test
    public void testKeepAlive() throws IOException, TimeoutException, InterruptedException {
        UniqueDatabase customerDatabase =
                new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
        customerDatabase.createAndInitialize();
        Properties dbzProps = new Properties();
        MySqlSourceConfig sourceConfig =
                getConfigFactory(MYSQL_CONTAINER, customerDatabase, new String[] {"customers"})
                        .startupOptions(StartupOptions.latest())
                        .debeziumProperties(dbzProps)
                        .createConfig(0);
        BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
        binaryLogClient.setKeepAliveInterval(10000);
        binaryLogClient.connect(10000);
        // wait for keepalive thread start.
        Thread.sleep(2000);
        // Close container to simulate connection timeout.
        stopContainers();
        while (binaryLogClient.getKeepAliveExceptionReference().get() == null) {
            Thread.sleep(1000);
        }
        Assert.assertTrue(
                binaryLogClient
                        .getKeepAliveExceptionReference()
                        .get()
                        .getMessage()
                        .contains("Create connection failed after retry"));
    }

    private MySqlSourceConfigFactory getConfigFactory(
            MySqlContainer container, UniqueDatabase database, String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .hostname(container.getHost())
                .port(container.getDatabasePort())
                .username(database.getUsername())
                .splitSize(4)
                .fetchSize(2)
                .password(database.getPassword());
    }
}
