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

package org.apache.flink.cdc.connectors.mysql.debezium.task.context;

import org.apache.flink.cdc.connectors.mysql.debezium.task.context.exception.SchemaOutOfSyncException;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;

import com.github.shyiko.mysql.binlog.network.ServerException;
import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A subclass implementation of {@link ErrorHandler} which filter some {@link DebeziumException}, we
 * use this class instead of {@link io.debezium.connector.mysql.MySqlErrorHandler}.
 */
public class MySqlErrorHandler extends ErrorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlErrorHandler.class);
    private static final Pattern NOT_FOUND_TABLE_MSG_PATTERN =
            Pattern.compile(
                    "Encountered change event for table (.+)\\.(.+) whose schema isn't known to this connector");
    private static final String SQL_CODE_TOO_MANY_CONNECTIONS = "08004";

    private final MySqlTaskContext context;
    private final MySqlSourceConfig sourceConfig;

    public MySqlErrorHandler(
            MySqlConnectorConfig mySqlConnectorConfig,
            ChangeEventQueue<?> queue,
            MySqlTaskContext context,
            MySqlSourceConfig sourceConfig) {
        super(MySqlConnector.class, mySqlConnectorConfig, queue);
        this.context = context;
        this.sourceConfig = sourceConfig;
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof SQLException) {
            final SQLException sql = (SQLException) throwable;
            if (SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSQLState())) {
                LOG.warn("Retry to connect when sql state is " + SQL_CODE_TOO_MANY_CONNECTIONS);
            }
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSQLState());
        } else if (throwable instanceof ServerException) {
            final ServerException sql = (ServerException) throwable;
            if (SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSqlState())) {
                LOG.warn("Retry to connect when sql state is " + SQL_CODE_TOO_MANY_CONNECTIONS);
            }
            return SQL_CODE_TOO_MANY_CONNECTIONS.equals(sql.getSqlState());
        } else if (throwable instanceof EOFException) {
            // Retry with reading binlog error
            if (throwable.getMessage().contains("Failed to read next byte from position")) {
                LOG.error(
                        "Retry to connect when there is some error when reading binlog.",
                        throwable);
            }
            return throwable.getMessage().contains("Failed to read next byte from position");
        } else if (throwable instanceof DebeziumException && throwable.getCause() != null) {
            return isRetriable(throwable.getCause());
        }
        return false;
    }

    @Override
    public void setProducerThrowable(Throwable producerThrowable) {
        if (isTableNotFoundException(producerThrowable)) {
            Matcher matcher =
                    NOT_FOUND_TABLE_MSG_PATTERN.matcher(producerThrowable.getCause().getMessage());
            String databaseName = matcher.group(1);
            String tableName = matcher.group(2);
            TableId tableId = new TableId(databaseName, null, tableName);
            if (context.getSchema().schemaFor(tableId) == null) {
                LOG.warn("Schema for table " + tableId + " is null");
                return;
            }
        }

        if (isSchemaOutOfSyncException(producerThrowable)) {
            super.setProducerThrowable(
                    new SchemaOutOfSyncException(
                            "Internal schema representation is probably out of sync with real database schema. "
                                    + "The reason could be that the table schema was changed after the starting "
                                    + "binlog offset, which is not supported when startup mode is set to "
                                    + sourceConfig.getStartupOptions().startupMode,
                            producerThrowable));
            return;
        }

        super.setProducerThrowable(producerThrowable);
    }

    private boolean isTableNotFoundException(Throwable t) {
        if (!(t.getCause() instanceof DebeziumException)) {
            return false;
        }
        DebeziumException e = (DebeziumException) t.getCause();
        String detailMessage = e.getMessage();
        Matcher matcher = NOT_FOUND_TABLE_MSG_PATTERN.matcher(detailMessage);
        return matcher.find();
    }

    private boolean isSchemaOutOfSyncException(Throwable t) {
        Throwable rootCause = ExceptionUtils.getRootCause(t);
        return rootCause instanceof ConnectException
                && rootCause
                        .getMessage()
                        .endsWith(
                                "internal schema representation is probably out of sync with real database schema")
                && sourceConfig.getStartupOptions().isStreamOnly();
    }
}
