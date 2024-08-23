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

import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for RDS related logic. */
public class AliyunRdsUtils {
    public static boolean needToReadRdsArchives(
            StatefulTaskContext statefulTaskContext, BinlogOffset startingOffset) {
        if (!statefulTaskContext.getSourceConfig().isReadRdsArchivedBinlogEnabled()) {
            return false;
        }
        String earliestBinlogFilenameInLocal =
                statefulTaskContext.getConnection().earliestBinlogFilename();
        if (startingOffset.getFilename() != null) {
            return startingOffset.getFilename().compareToIgnoreCase(earliestBinlogFilenameInLocal)
                    < 0;
        } else {
            String latestBinlogFilenameFromOss =
                    getLatestBinlogFilenameFromOss(statefulTaskContext, startingOffset);
            if (latestBinlogFilenameFromOss == null) {
                return false;
            }
            if (earliestBinlogFilenameInLocal.compareTo(latestBinlogFilenameFromOss) > 0) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static String getLatestBinlogFilenameFromOss(
            StatefulTaskContext statefulTaskContext, BinlogOffset startingOffset) {
        checkNotNull(
                statefulTaskContext.getSourceConfig().getRdsConfig(),
                "AliyunRdsBinlogFileFetcher requires AliyunRdsConfig");
        try (AliyunRdsBinlogFileFetcher aliyunRdsBinlogFileFetcher =
                new AliyunRdsBinlogFileFetcher(
                        statefulTaskContext.getSourceConfig().getRdsConfig(),
                        0,
                        startingOffset.getTimestampSec() * 1000)) {
            return aliyunRdsBinlogFileFetcher.getLatestBinlogFilename();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get latest binlog filename from oss", e);
        }
    }

    public static AliyunRdsSwitchingBinlogReadingTaskContext createRdsSwitchingContext(
            StatefulTaskContext statefulTaskContext, BinlogOffset startingOffset) {
        checkNotNull(
                statefulTaskContext.getSourceConfig().getRdsConfig(),
                "RDS switching context requires AliyunRdsConfig");
        try {
            return new AliyunRdsSwitchingBinlogReadingTaskContext(
                    statefulTaskContext.getConnectorConfig(),
                    statefulTaskContext.getDatabaseSchema(),
                    statefulTaskContext.getSourceConfig().getRdsConfig(),
                    startingOffset.getTimestampSec() * 1000,
                    System.currentTimeMillis(),
                    startingOffset,
                    statefulTaskContext.getConnection());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create RDS switching context", e);
        }
    }
}
