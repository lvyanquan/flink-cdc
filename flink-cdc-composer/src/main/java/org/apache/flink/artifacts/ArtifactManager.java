/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.artifacts;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.artifacts.ArtifactFetcher;
import org.apache.flink.table.artifacts.ArtifactFinder;
import org.apache.flink.table.artifacts.ArtifactIdentifier;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.MutableURLClassLoader;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Manager to manage artifact fetcher and finder. Notice: the creation and close of fetcher and
 * finder is sql-gateway.
 */
public class ArtifactManager {
    private static final String FILE_SCHEME = "file";

    private ArtifactFetcher fetcher;
    private ArtifactFinder finder;
    private MutableURLClassLoader classLoader;

    public ArtifactManager(
            MutableURLClassLoader classLoader, ArtifactFetcher fetcher, ArtifactFinder finder) {

        this.classLoader = classLoader;
        this.fetcher = fetcher;
        this.finder = finder;
    }

    public List<URI> getArtifactRemoteUris(
            ArtifactIdentifier identifier, Map<String, String> options) {
        return finder.getArtifactUri(identifier, options);
    }

    /**
     * Copied from
     * org.apache.flink.table.resource.ResourceManager#registerJarResources(java.util.List,
     * boolean).
     *
     * @param resources
     * @throws IOException
     */
    public void registerArtifactResources(List<URI> resources) throws IOException {
        // download resource to local path firstly if in remote
        URL localUrl;
        for (URI resource : resources) {
            String scheme = org.apache.commons.lang3.StringUtils.lowerCase(resource.getScheme());
            // download resource to local path firstly if in remote
            if (scheme != null && !FILE_SCHEME.equals(scheme)) {
                // TODO download resources with batch. See AONE-55659871
                Path localPath = fetcher.fetchArtifacts(Collections.singletonList(resource)).get(0);
                localUrl = localPath.toUri().toURL();
            } else {
                localUrl = getURLFromPath(new Path(resource));
            }

            JarUtils.checkJarFile(localUrl);

            // register to classLoader
            classLoader.addURL(localUrl);
        }
    }

    /** Get the {@link URL} from the {@link Path}. */
    public static URL getURLFromPath(Path path) throws IOException {
        return addDefaultPathSchemaIfNecessary(path).toUri().toURL();
    }

    /** If the scheme in origin path is null, rewrite it to file. */
    public static Path addDefaultPathSchemaIfNecessary(Path path) {
        if (path.toUri().getScheme() == null) {
            path = path.makeQualified(FileSystem.getLocalFileSystem());
        }
        return path;
    }
}
