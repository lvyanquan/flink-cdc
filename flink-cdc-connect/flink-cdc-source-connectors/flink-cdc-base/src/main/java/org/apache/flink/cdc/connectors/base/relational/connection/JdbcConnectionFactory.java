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

package org.apache.flink.cdc.connectors.base.relational.connection;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/** A factory to create JDBC connection. */
public class JdbcConnectionFactory implements JdbcConnection.ConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionFactory.class);

    private final JdbcSourceConfig sourceConfig;
    private final JdbcConnectionPoolFactory jdbcConnectionPoolFactory;

    public JdbcConnectionFactory(
            JdbcSourceConfig sourceConfig, JdbcConnectionPoolFactory jdbcConnectionPoolFactory) {
        this.sourceConfig = sourceConfig;
        this.jdbcConnectionPoolFactory = jdbcConnectionPoolFactory;
    }

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        final int connectRetryTimes = sourceConfig.getConnectMaxRetries();

        final ConnectionPoolId connectionPoolId =
                jdbcConnectionPoolFactory.getPoolId(
                        config, jdbcConnectionPoolFactory.getClass().getName());

        int poolRecreationCount = 0;
        SQLException lastException = null;

        while (poolRecreationCount < connectRetryTimes) {
            HikariDataSource dataSource =
                    JdbcConnectionPools.getInstance(jdbcConnectionPoolFactory)
                            .getOrCreateConnectionPool(connectionPoolId, sourceConfig);

            int connectionAttempt = 0;
            while (connectionAttempt < connectRetryTimes) {
                try {
                    return dataSource.getConnection();
                } catch (SQLException e) {
                    lastException = e;
                    // Check if the pool was closed concurrently (e.g., during failover)
                    if (dataSource.isClosed()) {
                        LOG.info(
                                "Connection pool {} was closed concurrently, retrying to get or create pool (attempt {})",
                                connectionPoolId,
                                poolRecreationCount + 1);
                        // Break inner loop to retry getting/creating the pool
                        break;
                    }

                    if (connectionAttempt < connectRetryTimes - 1) {
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException ie) {
                            throw new FlinkRuntimeException(
                                    "Failed to get connection, interrupted while doing another attempt",
                                    ie);
                        }
                        LOG.warn("Get connection failed, retry times {}", connectionAttempt + 1);
                    }
                }
                connectionAttempt++;
            }

            // If pool was closed concurrently, increment pool recreation counter and retry
            if (dataSource.isClosed()) {
                poolRecreationCount++;
                continue;
            }
            // Pool is not closed but we exhausted connection attempts
            LOG.error("Get connection failed after retry {} times", connectRetryTimes);
            throw new FlinkRuntimeException(lastException);
        }

        LOG.error(
                "Failed to get connection after {} pool recreation attempts", poolRecreationCount);
        throw new FlinkRuntimeException(lastException);
    }
}
