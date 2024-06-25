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

package org.apache.flink.cdc.connectors.hologres.utils;

import com.alibaba.hologres.client.Command;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.SSLMode;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;

/** copy from JDBCUtils. */
public class JDBCUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);
    public static final String SHARD_ID_COLUMN_NAME = "hg_shard_id";
    public static final String SEQUENCE_NUMBER_COLUMN_NAME = "hg_sequence_number";

    public static String getDbUrl(String endpoint, String db) {
        return "jdbc:hologres://" + endpoint + "/" + db;
    }

    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    public static int getShardSize(HoloConfig config, String tableName) {
        DriverManager.getDrivers();
        try (HoloClient client = new HoloClient(config)) {
            return Command.getShardCount(client, client.getTableSchema(tableName));
        } catch (HoloClientException e) {
            throw new RuntimeException("Failed to get shard size.", e);
        }
    }

    public static String getTableId(HoloConfig config, String tableName) {
        DriverManager.getDrivers();
        try (HoloClient client = new HoloClient(config)) {
            return client.getTableSchema(tableName).getTableId();
        } catch (HoloClientException e) {
            throw new RuntimeException("Failed to get table id.", e);
        }
    }

    public static Connection createConnection(HoloConfig config) {
        return createConnection(config, config.getJdbcUrl(), false);
    }

    // use special jdbc url
    public static Connection createConnection(
            HoloConfig config, String url, boolean sslModeConnection) {
        try {
            DriverManager.getDrivers();
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            Properties properties = new Properties();
            PGProperty.USER.set(properties, config.getUsername());
            PGProperty.PASSWORD.set(properties, config.getPassword());
            PGProperty.APPLICATION_NAME.set(properties, "flink-cdc-pipeline-connector-hologres");
            PGProperty.SOCKET_TIMEOUT.set(properties, 360);
            if (sslModeConnection && config.getSslMode() != SSLMode.DISABLE) {
                PGProperty.SSL.set(properties, true);
                PGProperty.SSL_MODE.set(properties, config.getSslMode().getPgPropertyValue());
                if (config.getSslMode() == SSLMode.VERIFY_CA
                        || config.getSslMode() == SSLMode.VERIFY_FULL) {
                    if (config.getSslRootCertLocation() == null) {
                        throw new InvalidParameterException(
                                "When SSL_MODE is set to VERIFY_CA or VERIFY_FULL, the location of the ssl root certificate must be configured.");
                    }
                    PGProperty.SSL_ROOT_CERT.set(properties, config.getSslRootCertLocation());
                }
            }
            return DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed getting connection to %s because %s",
                            url, ExceptionUtils.getStackTrace(e)));
        }
    }

    public static String getFrontendEndpoints(HoloConfig holoConfig) throws SQLException {
        try (Connection connection = JDBCUtils.createConnection(holoConfig);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW hg_frontend_endpoints;")) {
            String endpointStr = null;
            while (rs.next()) {
                endpointStr = rs.getString(1);
                break;
            }
            if (endpointStr == null || endpointStr.isEmpty()) {
                throw new RuntimeException("Failed to query hg_frontend_endpoints.");
            }
            return endpointStr;
        }
    }

    public static int getNumberFrontends(HoloConfig options) {
        try (Connection connection = JDBCUtils.createConnection(options);
                Statement statement = connection.createStatement()) {
            int maxConnections = 128;
            try (ResultSet rs = statement.executeQuery("show max_connections;")) {
                if (rs.next()) {
                    maxConnections = rs.getInt(1);
                }
            }
            int instanceMaxConnections = 0;
            try (ResultSet rs = statement.executeQuery("select instance_max_connections();")) {
                if (rs.next()) {
                    instanceMaxConnections = rs.getInt(1);
                }
            }
            return instanceMaxConnections / maxConnections;
        } catch (SQLException e) {
            // function instance_max_connections is only supported for hologres version > 1.3.20.
            if (e.getMessage().contains("function instance_max_connections() does not exist")) {
                LOG.warn("Failed to get hologres frontends number.", e);
                return 0;
            }
            throw new RuntimeException("Failed to get hologres frontends number.", e);
        }
    }

    public static String getJdbcDirectConnectionUrl(HoloConfig holoConfig, String url) {
        // Returns the jdbc url directly connected to fe, which is used in none public cloud env,
        // traffic does not go through vip.
        LOG.info("Try to connect {} for getting fe endpoint", url);
        String endpoint = null;
        try (Connection conn = createConnection(holoConfig, url, /*sslModeConnection*/ false)) {
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery("select inet_server_addr(), inet_server_port()")) {
                    while (rs.next()) {
                        endpoint = rs.getString(1) + ":" + rs.getString(2);
                        break;
                    }
                    if (Objects.isNull(endpoint)) {
                        throw new RuntimeException(
                                "Failed to query \"select inet_server_addr(), inet_server_port()\".");
                    }
                }
            }
        } catch (SQLException t) {
            throw new RuntimeException(t);
        }
        return holoConfig.getJdbcUrl();
    }
}
