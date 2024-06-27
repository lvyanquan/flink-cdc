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

package org.apache.flink.cdc.pipeline.connector.distro;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.DumperOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class for generating <code>flink-cdc-pipeline-connector-meta.yaml</code> under the build target.
 */
public class AssembledMetaFileGenerator {

    // YAML keys
    private static final String DEPENDENCIES_KEY = "dependencies";
    private static final String PIPELINE_CONNECTORS_KEY = "pipeline-connectors";

    public static void main(String[] args) throws Exception {
        Path binaryPath = Paths.get(ParameterTool.fromArgs(args).get("binaryPath"));
        Path outputPath = Paths.get(ParameterTool.fromArgs(args).get("outputPath"));
        if (!Files.exists(binaryPath)) {
            throw new IllegalArgumentException(
                    String.format("Cannot find binary target at %s", binaryPath));
        }
        if (!Files.isDirectory(binaryPath)) {
            throw new IllegalArgumentException(
                    String.format("The binary target at %s is not a directory", binaryPath));
        }

        // Paths of meta files and dependencies should be a relative one against "flink"
        Path relativeRoot = binaryPath.resolve("flink-cdc");
        Path pipelineConnectorPath = relativeRoot.resolve("opt").resolve("pipeline-connectors");

        Map<String, Object> pipelineConnectorsToYaml = new HashMap<>();
        try (Stream<Path> stream = Files.list(pipelineConnectorPath)) {
            stream.forEach(
                    entityPath -> {
                        String identifier = entityPath.getFileName().toString();
                        List<Path> dependencies = getJars(entityPath, relativeRoot);
                        Map<String, Object> pipelineConnectorDetails = new HashMap<>();
                        pipelineConnectorDetails.put(
                                DEPENDENCIES_KEY,
                                dependencies.stream()
                                        .map(Path::toString)
                                        .collect(Collectors.toList()));
                        pipelineConnectorsToYaml.put(identifier, pipelineConnectorDetails);
                    });
        }
        Map<String, Object> toYaml = new HashMap<>();
        toYaml.put(PIPELINE_CONNECTORS_KEY, pipelineConnectorsToYaml);

        DumperOptions options = new DumperOptions();
        // Use block style
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        // Add indent before indicator "-"
        options.setIndentWithIndicator(true);
        options.setIndicatorIndent(2);

        Yaml yaml = new Yaml(options);
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            yaml.dump(toYaml, writer);
        }
    }

    private static List<Path> getJars(Path entityPath, Path relativeRoot) {
        try (Stream<Path> stream = Files.list(entityPath)) {
            return stream.filter(path -> path.getFileName().toString().endsWith("jar"))
                    .map(relativeRoot::relativize)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Error listing connector path %s", entityPath));
        }
    }
}
