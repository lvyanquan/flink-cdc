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

package org.apache.flink.cdc.rest;

import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.gateway.service.YamlServiceImpl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.artifacts.ArtifactFetcher;
import org.apache.flink.table.artifacts.ArtifactFinder;
import org.apache.flink.table.artifacts.ArtifactIdentifier;
import org.apache.flink.table.artifacts.ArtifactKind;
import org.apache.flink.util.MutableURLClassLoader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.flink.cdc.gateway.service.YamlServiceImpl} . */
public class YamlServiceTest {

    @Test
    public void testParallelismNotValid() throws Exception {
        String yamlContent =
                "source:\n"
                        + "   type: values\n"
                        + "   name: ValuesSource\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "  name: Values Sink\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  name: Sync Value Database to value\n"
                        + "  parallelism: 8";
        org.apache.flink.configuration.Configuration configuration =
                new Configuration().set(CoreOptions.DEFAULT_PARALLELISM, 6);
        PipelineExecution pipelineExecution =
                YamlServiceImpl.composeExecution(
                        yamlContent,
                        configuration,
                        new MockMutableURLClassLoader(
                                Thread.currentThread().getContextClassLoader()),
                        new MockArtifactFetcher(),
                        new MockArtifactFinder());
        String executionPlan = pipelineExecution.getExecutionPlan();
        assertThat(executionPlan).contains("\"parallelism\" : 6");
        assertThat(executionPlan).doesNotContain("\"parallelism\" : 8");
    }

    private static class MockMutableURLClassLoader extends MutableURLClassLoader {
        public MockMutableURLClassLoader(ClassLoader parent) {
            super(new URL[] {}, parent);
        }

        static {
            ClassLoader.registerAsParallelCapable();
        }

        @Override
        public MutableURLClassLoader copy() {
            return null;
        }
    }

    private static class MockArtifactFetcher implements ArtifactFetcher {
        @Override
        public void close() throws IOException {}

        @Override
        public List<Path> fetchArtifacts(List<URI> list) throws IOException {
            return Collections.emptyList();
        }
    }

    private static class MockArtifactFinder implements ArtifactFinder {
        @Override
        public List<URI> getArtifactUri(
                ArtifactIdentifier artifactIdentifier, Map<String, String> map) {
            return Collections.emptyList();
        }

        @Override
        public List<ArtifactIdentifier> listArtifacts(List<ArtifactKind> list) {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {}
    }
}
