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

package org.apache.flink.cdc.connectors.mysql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/** Utils about files to help test. */
public class FileUtils {

    public static final String ORIGIN_PATH = System.getenv("ORIGIN_PATH");
    public static final String CONTAINER_ROOT = System.getenv("CONTAINER_ROOT");
    public static final String ADMIN_UID = "10001";
    public static final Set<PosixFilePermission> TEMP_DIR_PERMISSION =
            PosixFilePermissions.fromString("rwxrwxr-x");

    private FileUtils() {}

    public static Path createTempDirectory(Path dir, String prefix) throws IOException {
        return Files.createTempDirectory(
                dir, prefix, PosixFilePermissions.asFileAttribute(TEMP_DIR_PERMISSION));
    }
}
