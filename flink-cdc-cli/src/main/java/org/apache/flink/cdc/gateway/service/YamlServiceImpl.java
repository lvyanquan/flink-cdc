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

package org.apache.flink.cdc.gateway.service;

import org.apache.flink.artifacts.ArtifactManager;
import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.artifacts.ArtifactFetcher;
import org.apache.flink.table.artifacts.ArtifactFinder;
import org.apache.flink.table.gateway.api.cdc.YamlService;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.CatalogOptionsInfo;
import org.apache.flink.table.gateway.api.vvr.command.info.cdc.IdentifierInfo;
import org.apache.flink.util.MutableURLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.PipelineOptions.JARS;

/** CDC Ability. */
public class YamlServiceImpl implements YamlService {
    private static final Logger LOG = LoggerFactory.getLogger(YamlServiceImpl.class);

    @Override
    public void verifyCdcYaml(
            String yamlContent,
            Configuration flinkConfig,
            MutableURLClassLoader mutableURLClassLoader,
            ArtifactFetcher artifactFetcher,
            ArtifactFinder artifactFinder)
            throws Exception {
        PipelineExecution pipelineExecution =
                composeExecution(
                        yamlContent,
                        flinkConfig,
                        mutableURLClassLoader,
                        artifactFetcher,
                        artifactFinder);
        pipelineExecution.getExecutionPlan();
    }

    @Override
    public void deployCdcYaml(
            String yamlContent,
            Configuration flinkConfig,
            MutableURLClassLoader mutableURLClassLoader,
            ArtifactFetcher artifactFetcher,
            ArtifactFinder artifactFinder)
            throws Exception {
        PipelineExecution pipelineExecution =
                composeExecution(
                        yamlContent,
                        flinkConfig,
                        mutableURLClassLoader,
                        artifactFetcher,
                        artifactFinder);
        pipelineExecution.execute();
    }

    @Override
    public IdentifierInfo getReferencedCatalogInfo(
            List<Object> list,
            Configuration configuration,
            MutableURLClassLoader mutableURLClassLoader,
            ArtifactFetcher artifactFetcher,
            ArtifactFinder artifactFinder)
            throws Exception {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public String convertCxasToCdcYaml(
            List<Object> list,
            CatalogOptionsInfo catalogOptionsInfo,
            Configuration configuration,
            MutableURLClassLoader mutableURLClassLoader,
            ArtifactFetcher artifactFetcher,
            ArtifactFinder artifactFinder)
            throws Exception {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @VisibleForTesting
    public static PipelineExecution composeExecution(
            String yamlContent,
            org.apache.flink.configuration.Configuration flinkConfig,
            MutableURLClassLoader mutableURLClassLoader,
            ArtifactFetcher artifactFetcher,
            ArtifactFinder artifactFinder)
            throws Exception {
        // read config from flink
        flinkConfig =
                (org.apache.flink.configuration.Configuration)
                        StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig)
                                .getConfiguration();

        org.apache.flink.cdc.common.configuration.Configuration cdcConfiguration =
                org.apache.flink.cdc.common.configuration.Configuration.fromMap(
                        flinkConfig.toMap());
        // if used in vvr, ignore pipeline parallelism in yaml
        cdcConfiguration.set(PipelineOptions.IGNORE_PIPELINE_PARALLELISM, true);

        // Parse pipeline definition file
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        // application mode will auto read yaml.config, thus pass an empty here
        // but we want to read
        PipelineDef pipelineDef = pipelineDefinitionParser.parse(yamlContent, cdcConfiguration);

        LOG.info("jarPath from local path is: " + flinkConfig.get(JARS));
        // Create composer
        PipelineComposer composer =
                FlinkPipelineComposer.ofApplicationCluster(
                        flinkConfig,
                        Collections.EMPTY_LIST,
                        mutableURLClassLoader,
                        new ArtifactManager(
                                mutableURLClassLoader, artifactFetcher, artifactFinder));

        // Compose pipeline
        return composer.compose(pipelineDef);
    }
}
