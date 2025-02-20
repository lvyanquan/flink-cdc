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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.artifacts.ArtifactManager;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkDataStreamSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkFunctionProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.FlinkEnvironmentUtils;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkFunctionOperator;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.table.artifacts.ArtifactIdentifier;
import org.apache.flink.table.artifacts.ArtifactKind;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

/** Translator used to build {@link DataSink} for given {@link DataStream}. */
@Internal
public class DataSinkTranslator {

    private static final String SINK_WRITER_PREFIX = "Sink Writer: ";
    private static final String SINK_COMMITTER_PREFIX = "Sink Committer: ";

    private @Nullable ArtifactManager artifactManager;

    ClassLoader classLoader;

    public DataSinkTranslator(ArtifactManager artifactManager, ClassLoader classLoader) {
        this.artifactManager = artifactManager;
        this.classLoader = classLoader;
    }

    public DataSinkTranslator() {
        this(null, Thread.currentThread().getContextClassLoader());
    }

    public DataSink createDataSink(
            SinkDef sinkDef, Configuration pipelineConfig, StreamExecutionEnvironment env) {
        // Search the data sink factory
        DataSinkFactory sinkFactory = findSinkFactory(sinkDef.getType(), env);

        // Create data sink
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        sinkDef.getConfig(),
                        pipelineConfig,
                        Thread.currentThread().getContextClassLoader(),
                        env.getConfiguration()));
    }

    public void translate(
            SinkDef sinkDef,
            DataStream<Event> input,
            DataSink dataSink,
            OperatorID schemaOperatorID) {
        // Get sink provider
        EventSinkProvider eventSinkProvider = dataSink.getEventSinkProvider();
        String sinkName = generateSinkName(sinkDef);
        if (eventSinkProvider instanceof FlinkSinkProvider) {
            // Sink V2
            FlinkSinkProvider sinkProvider = (FlinkSinkProvider) eventSinkProvider;
            Sink<Event> sink = sinkProvider.getSink();
            sinkTo(input, sink, sinkName, schemaOperatorID);
        } else if (eventSinkProvider instanceof FlinkSinkFunctionProvider) {
            // SinkFunction
            FlinkSinkFunctionProvider sinkFunctionProvider =
                    (FlinkSinkFunctionProvider) eventSinkProvider;
            SinkFunction<Event> sinkFunction = sinkFunctionProvider.getSinkFunction();
            sinkTo(input, sinkFunction, sinkName, schemaOperatorID);
        } else if (eventSinkProvider instanceof FlinkDataStreamSinkProvider) {
            // SinkFunction
            FlinkDataStreamSinkProvider dataStreamSinkProvider =
                    (FlinkDataStreamSinkProvider) eventSinkProvider;
            Tuple2<DataStream<Event>, Sink<Event>> result =
                    dataStreamSinkProvider.consumeDataStream(input);
            sinkTo(result.f0, result.f1, sinkName, schemaOperatorID);
        }
    }

    @VisibleForTesting
    void sinkTo(
            DataStream<Event> input,
            Sink<Event> sink,
            String sinkName,
            OperatorID schemaOperatorID) {
        DataStream<Event> stream = input;
        // Pre-write topology
        if (sink instanceof WithPreWriteTopology) {
            stream = ((WithPreWriteTopology<Event>) sink).addPreWriteTopology(stream);
        }

        if (sink instanceof TwoPhaseCommittingSink) {
            addCommittingTopology(sink, stream, sinkName, schemaOperatorID);
        } else {
            stream.transform(
                    SINK_WRITER_PREFIX + sinkName,
                    CommittableMessageTypeInfo.noOutput(),
                    new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID));
        }
    }

    private void sinkTo(
            DataStream<Event> input,
            SinkFunction<Event> sinkFunction,
            String sinkName,
            OperatorID schemaOperatorID) {
        DataSinkFunctionOperator sinkOperator =
                new DataSinkFunctionOperator(sinkFunction, schemaOperatorID);
        final StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
        PhysicalTransformation<Event> transformation =
                new LegacySinkTransformation<>(
                        input.getTransformation(),
                        SINK_WRITER_PREFIX + sinkName,
                        sinkOperator,
                        executionEnvironment.getParallelism(),
                        false);
        executionEnvironment.addOperator(transformation);
    }

    private <CommT> void addCommittingTopology(
            Sink<Event> sink,
            DataStream<Event> inputStream,
            String sinkName,
            OperatorID schemaOperatorID) {
        TypeInformation<CommittableMessage<CommT>> typeInformation =
                CommittableMessageTypeInfo.of(() -> getCommittableSerializer(sink));
        DataStream<CommittableMessage<CommT>> written =
                inputStream.transform(
                        SINK_WRITER_PREFIX + sinkName,
                        typeInformation,
                        new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID));

        DataStream<CommittableMessage<CommT>> preCommitted = written;
        if (sink instanceof WithPreCommitTopology) {
            preCommitted =
                    ((WithPreCommitTopology<Event, CommT>) sink).addPreCommitTopology(written);
        }

        // TODO: Hard coding stream mode and checkpoint
        boolean isBatchMode = false;
        boolean isCheckpointingEnabled = true;
        DataStream<CommittableMessage<CommT>> committed =
                preCommitted.transform(
                        SINK_COMMITTER_PREFIX + sinkName,
                        typeInformation,
                        getCommitterOperatorFactory(sink, isBatchMode, isCheckpointingEnabled));

        if (sink instanceof WithPostCommitTopology) {
            ((WithPostCommitTopology<Event, CommT>) sink).addPostCommitTopology(committed);
        }
    }

    private String generateSinkName(SinkDef sinkDef) {
        return sinkDef.getName()
                .orElse(String.format("Flink CDC Event Sink: %s", sinkDef.getType()));
    }

    private static <CommT> SimpleVersionedSerializer<CommT> getCommittableSerializer(Object sink) {
        // FIX ME: TwoPhaseCommittingSink has been deprecated, and its signature has changed
        // during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (SimpleVersionedSerializer<CommT>)
                    sink.getClass().getDeclaredMethod("getCommittableSerializer").invoke(sink);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to get CommittableSerializer", e);
        }
    }

    private static <CommT>
            OneInputStreamOperatorFactory<CommittableMessage<CommT>, CommittableMessage<CommT>>
                    getCommitterOperatorFactory(
                            Sink<Event> sink, boolean isBatchMode, boolean isCheckpointingEnabled) {
        // FIX ME: OneInputStreamOperatorFactory is an @Internal class, and its signature has
        // changed during Flink 1.18 to 1.19. Remove this when Flink 1.18 is no longer supported.
        try {
            return (OneInputStreamOperatorFactory<
                            CommittableMessage<CommT>, CommittableMessage<CommT>>)
                    Class.forName(
                                    "org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory")
                            .getDeclaredConstructors()[0]
                            .newInstance(sink, isBatchMode, isCheckpointingEnabled);

        } catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException("Failed to create CommitterOperatorFactory", e);
        }
    }

    private DataSinkFactory findSinkFactory(String identifier, StreamExecutionEnvironment env) {
        DataSinkFactory sinkFactory = null;
        // try spi first to check if the classloader contains this connector jar
        try {
            sinkFactory =
                    FactoryDiscoveryUtils.getFactoryByIdentifier(
                            identifier, DataSinkFactory.class, classLoader);
        } catch (Exception ignored) {
            if (artifactManager == null) {
                throw new RuntimeException("No DataSourceFactory is found.");
            }
        }

        /** the dependency already in flink image is no need to add to classloader and add-jar. */
        if (sinkFactory != null) {
            return sinkFactory;
        }

        try {
            // 3. get the remote uris about this connector
            // although the remote uris may be empty, we also do step 4 and 5 to get full exception
            ArtifactIdentifier artifactIdentifier =
                    ArtifactIdentifier.of(ArtifactKind.PIPELINE_SINK, identifier);

            List<URI> artifactRemoteUris =
                    artifactManager.getArtifactRemoteUris(artifactIdentifier, new HashMap<>());

            // 4. register these uris to classloader
            artifactManager.registerArtifactResources(artifactRemoteUris);

            // Include sink connector JAR
            FactoryDiscoveryUtils.getJarPathByIdentifier(
                            identifier, DataSinkFactory.class, classLoader)
                    .ifPresent(jar -> FlinkEnvironmentUtils.addJar(env, jar));

            // 5. try spi again and throw full error messages
            sinkFactory =
                    FactoryDiscoveryUtils.getFactoryByIdentifier(
                            identifier, DataSinkFactory.class, classLoader);

            return sinkFactory;
        } catch (Exception exception) {
            throw new RuntimeException("fail to download and register sinkFactory.", exception);
        }
    }
}
