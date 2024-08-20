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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.artifacts.ArtifactManager;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.utils.OptionUtils;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.artifacts.ArtifactIdentifier;
import org.apache.flink.table.artifacts.ArtifactKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.OptionUtils.VVR_START_TIME_MS;

/** Translator used to build {@link DataSource} which will generate a {@link DataStream}. */
@Internal
public class DataSourceTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceTranslator.class);
    private @Nullable ArtifactManager artifactManager;

    ClassLoader classLoader;

    public DataSourceTranslator(ArtifactManager artifactManager, ClassLoader classLoader) {
        this.artifactManager = artifactManager;
        this.classLoader = classLoader;
    }

    public DataStreamSource<Event> translate(
            SourceDef sourceDef,
            StreamExecutionEnvironment env,
            Configuration pipelineConfig,
            int sourceParallelism) {

        String startTimeMs = OptionUtils.getStartTimeMs(env.getConfiguration());
        if (startTimeMs != null) {
            sourceDef = appendStartTimeMs(sourceDef, startTimeMs);
        }

        // Create data source
        DataSource dataSource = createDataSource(sourceDef, env, pipelineConfig);

        // Get source provider
        EventSourceProvider eventSourceProvider = dataSource.getEventSourceProvider();
        if (eventSourceProvider instanceof FlinkSourceProvider) {
            // Source
            FlinkSourceProvider sourceProvider = (FlinkSourceProvider) eventSourceProvider;
            return env.fromSource(
                            sourceProvider.getSource(),
                            WatermarkStrategy.noWatermarks(),
                            sourceDef.getName().orElse(generateDefaultSourceName(sourceDef)),
                            new EventTypeInfo())
                    .setParallelism(sourceParallelism);
        } else if (eventSourceProvider instanceof FlinkSourceFunctionProvider) {
            // SourceFunction
            FlinkSourceFunctionProvider sourceFunctionProvider =
                    (FlinkSourceFunctionProvider) eventSourceProvider;
            DataStreamSource<Event> stream =
                    env.addSource(sourceFunctionProvider.getSourceFunction(), new EventTypeInfo())
                            .setParallelism(sourceParallelism);
            if (sourceDef.getName().isPresent()) {
                stream.name(sourceDef.getName().get());
            }
            return stream;
        } else {
            // Unknown provider type
            throw new IllegalStateException(
                    String.format(
                            "Unsupported EventSourceProvider type \"%s\"",
                            eventSourceProvider.getClass().getCanonicalName()));
        }
    }

    private DataSource createDataSource(
            SourceDef sourceDef, StreamExecutionEnvironment env, Configuration pipelineConfig) {
        // Search the data source factory
        DataSourceFactory sourceFactory = findSourceFactory(sourceDef.getType(), env);
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                sourceDef.getConfig(), pipelineConfig, classLoader));
        return dataSource;
    }

    private String generateDefaultSourceName(SourceDef sourceDef) {
        return String.format("Flink CDC Event Source: %s", sourceDef.getType());
    }

    private SourceDef appendStartTimeMs(SourceDef sourceDef, String startTimeMs) {
        Map<String, String> conf = sourceDef.getConfig().toMap();
        conf.put(VVR_START_TIME_MS.key(), startTimeMs);
        return new SourceDef(
                sourceDef.getType(), sourceDef.getName().orElse(null), Configuration.fromMap(conf));
    }

    private DataSourceFactory findSourceFactory(String identifier, StreamExecutionEnvironment env) {
        DataSourceFactory sourceFactory = null;
        // try spi first to check if the classloader contains this connector jar
        try {
            sourceFactory =
                    FactoryDiscoveryUtils.getFactoryByIdentifier(
                            identifier, DataSourceFactory.class, classLoader);
        } catch (Exception ignored) {
            if (artifactManager == null) {
                throw new RuntimeException("No DataSourceFactory is found.");
            }
        }

        /** the dependency already in flink image is no need to add to classloader and add-jar. */
        if (sourceFactory != null) {
            return sourceFactory;
        }

        try {
            // 3. get the remote uris about this connector
            // although the remote uris may be empty, we also do step 4 and 5 to get full exception
            ArtifactIdentifier artifactIdentifier =
                    ArtifactIdentifier.of(ArtifactKind.PIPELINE_SOURCE, identifier);

            List<URI> artifactRemoteUris =
                    artifactManager.getArtifactRemoteUris(artifactIdentifier, new HashMap<>());

            // 4. register these uris to classloader
            artifactManager.registerArtifactResources(artifactRemoteUris);
            // Add source JAR to environment.
            FactoryDiscoveryUtils.getJarPathByIdentifier(
                            identifier, DataSourceFactory.class, classLoader)
                    .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

            // 5. try spi again and throw full error messages
            sourceFactory =
                    FactoryDiscoveryUtils.getFactoryByIdentifier(
                            identifier, DataSourceFactory.class, classLoader);

            return sourceFactory;
        } catch (Exception exception) {
            LOG.error("fail to download and register sourceFactor", exception);
            throw new RuntimeException("fail to download and register sourceFactory.", exception);
        }
    }
}
