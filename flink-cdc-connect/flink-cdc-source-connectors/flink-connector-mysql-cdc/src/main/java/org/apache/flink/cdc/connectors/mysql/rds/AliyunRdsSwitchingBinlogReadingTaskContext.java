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

package org.apache.flink.cdc.connectors.mysql.rds;

import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.SwitchingBinaryLogClient;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTaskContext;

/**
 * A {@link MySqlTaskContext} that could provide a {@link SwitchingBinaryLogClient} in order to
 * fetch and read RDS archived binlog files then switch to online binlogs.
 */
public class AliyunRdsSwitchingBinlogReadingTaskContext extends MySqlTaskContext {
    public final AliyunRdsBinlogFileFetcher aliyunRdsBinlogFileFetcher;
    public final SwitchingBinaryLogClient client;

    public AliyunRdsSwitchingBinlogReadingTaskContext(
            MySqlConnectorConfig config,
            MySqlDatabaseSchema schema,
            AliyunRdsConfig rdsConfig,
            long startingTimestampMs,
            long stoppingTimestampMs,
            BinlogOffset startingOffset,
            MySqlConnection connection) {
        super(config, schema);
        this.aliyunRdsBinlogFileFetcher =
                new AliyunRdsBinlogFileFetcher(rdsConfig, startingTimestampMs, stoppingTimestampMs);
        String earliestBinlogFilenameOnArchive =
                aliyunRdsBinlogFileFetcher.initialize(startingOffset);

        if (earliestBinlogFilenameOnArchive == null) {
            throw new IllegalStateException("No binlog files are available on RDS archives");
        }

        this.client =
                new SwitchingBinaryLogClient(
                        config.hostname(),
                        config.port(),
                        config.username(),
                        config.password(),
                        connection,
                        aliyunRdsBinlogFileFetcher,
                        startingOffset);
    }

    @Override
    public BinaryLogClient getBinaryLogClient() {
        return client;
    }
}
