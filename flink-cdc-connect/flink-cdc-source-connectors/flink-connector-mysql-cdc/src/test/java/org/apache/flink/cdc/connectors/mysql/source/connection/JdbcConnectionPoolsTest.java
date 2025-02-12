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

package org.apache.flink.cdc.connectors.mysql.source.connection;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceTestBase;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Test;

import java.sql.SQLException;

/** Test for {@link JdbcConnectionPools}. */
public class JdbcConnectionPoolsTest extends MySqlSourceTestBase {
    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @Test
    public void testUseMultipleJdbcConnectionsInSameTime() throws SQLException {
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname(inventoryDatabase.getHost())
                        .port(inventoryDatabase.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList("inventory")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .createConfig(0);
        JdbcConnectionPools connectionPools = JdbcConnectionPools.getInstance();
        HikariDataSource dataSource1 =
                connectionPools.getOrCreateConnectionPool(
                        new ConnectionPoolId(
                                config.getHostname(), config.getPort(), config.getUsername()),
                        config);
        HikariDataSource dataSource2 =
                connectionPools.getOrCreateConnectionPool(
                        new ConnectionPoolId(
                                config.getHostname(), config.getPort(), config.getUsername()),
                        config);
        dataSource1.getConnection();
        dataSource1.close();
        // test whether an exception will be thrown if datasource is closed.
        dataSource2.getConnection();
        dataSource2.close();
    }

    @Test
    public void testUseJdbcConnectionOneByOne() throws SQLException {
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname(inventoryDatabase.getHost())
                        .port(inventoryDatabase.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList("inventory")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .createConfig(0);
        JdbcConnectionPools connectionPools = JdbcConnectionPools.getInstance();
        HikariDataSource dataSource1 =
                connectionPools.getOrCreateConnectionPool(
                        new ConnectionPoolId(
                                config.getHostname(), config.getPort(), config.getUsername()),
                        config);
        dataSource1.getConnection();
        dataSource1.close();
        HikariDataSource dataSource2 =
                connectionPools.getOrCreateConnectionPool(
                        new ConnectionPoolId(
                                config.getHostname(), config.getPort(), config.getUsername()),
                        config);

        dataSource2.getConnection();
        dataSource2.close();
    }
}
