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

package org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog;

import org.apache.flink.annotation.Internal;

import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

/** A binlog file that stored in local file system. */
@Internal
public class LocalBinlogFile implements Comparable<LocalBinlogFile> {
    private final String filename;
    private final Path path;

    public LocalBinlogFile(String filename, Path path) {
        this.filename = filename;
        this.path = path;
    }

    public String getFilename() {
        return filename;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public int compareTo(@NotNull LocalBinlogFile that) {
        return filename.compareToIgnoreCase(that.filename);
    }

    @Override
    public String toString() {
        return "LocalBinlogFile{" + "filename='" + filename + '\'' + ", path=" + path + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LocalBinlogFile)) {
            return false;
        }
        LocalBinlogFile that = (LocalBinlogFile) o;
        return filename.equals(that.filename) && path.equals(that.path);
    }
}
