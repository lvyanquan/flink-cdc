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

package org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.LocalBinlogFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A binlog file fetcher that lists all binlog files in the specified directory and serve then in
 * order.
 */
@Internal
public class LocalBinlogFileFetcher implements BinlogFileFetcher {
    // TODO: Not all binlog filenames start with this prefix. This could be changed to configurable.
    private static final String MYSQL_BINLOG_FILE_PREFIX = "mysql-bin.0";
    private final PriorityQueue<LocalBinlogFile> binlogFileQueue = new PriorityQueue<>();

    public LocalBinlogFileFetcher(Path binlogFileDirectory) throws IOException {
        binlogFileQueue.addAll(discoverBinlogFiles(binlogFileDirectory));
    }

    @Override
    public boolean hasNext() {
        return !binlogFileQueue.isEmpty();
    }

    @Nullable
    @Override
    public LocalBinlogFile next() {
        return binlogFileQueue.poll();
    }

    @Override
    public void close() throws Exception {}

    // ------------------------------- Helper functions -------------------------
    private static List<LocalBinlogFile> discoverBinlogFiles(Path binlogFileDirectory)
            throws IOException {
        try (Stream<Path> directoryStream = Files.list(binlogFileDirectory)) {
            return directoryStream
                    .filter(Files::isRegularFile)
                    .filter(
                            file ->
                                    file.getFileName()
                                            .toString()
                                            .startsWith(MYSQL_BINLOG_FILE_PREFIX))
                    .map(
                            binlogFile ->
                                    new LocalBinlogFile(
                                            binlogFile.getFileName().toString(), binlogFile))
                    .collect(Collectors.toList());
        }
    }
}
