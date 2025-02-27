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

package org.apache.flink.cdc.connectors.sls.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.connectors.sls.source.enumerator.PipelineSlsSourceEnumState;
import org.apache.flink.cdc.connectors.sls.source.enumerator.PipelineSlsSourceEnumStateSerializer;
import org.apache.flink.cdc.connectors.sls.source.enumerator.PipelineSlsSourceEnumerator;
import org.apache.flink.cdc.connectors.sls.source.reader.PipelineSlsRecordEmitter;
import org.apache.flink.cdc.connectors.sls.source.reader.PipelineSlsSourceReader;
import org.apache.flink.cdc.connectors.sls.source.split.PipelineSlsShardSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.alibaba.ververica.connectors.sls.newsource.SlsSource;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.SlsEnumState;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.assigner.ShardAssigner;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.initializer.CursorInitializer;
import com.alibaba.ververica.connectors.sls.newsource.metrics.SlsSourceReaderMetrics;
import com.alibaba.ververica.connectors.sls.newsource.model.SlsSourceRecord;
import com.alibaba.ververica.connectors.sls.newsource.reader.SlsShardSplitReader;
import com.alibaba.ververica.connectors.sls.newsource.reader.fetcher.SlsSourceFetcherManager;
import com.alibaba.ververica.connectors.sls.newsource.reader.format.SlsLogGroupDeserializationSchema;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;

/**
 * The Source implementation of pipeline SLS. Please use {@link PipelineSlsSourceBuilder} to
 * construct a {@link PipelineSlsSource}.
 */
public class PipelineSlsSource extends SlsSource<Event> {
    private static final long serialVersionUID = 1L;

    private final int maxFetchLogGroups;
    private final SchemaInferenceStrategy schemaInferenceStrategy;

    public PipelineSlsSource(
            CursorInitializer startingCursorInitializer,
            CursorInitializer stoppingCursorInitializer,
            ShardAssigner shardAssigner,
            SlsLogGroupDeserializationSchema<Event> deserializationSchema,
            Configuration config,
            int maxFetchLogGroups,
            SchemaInferenceStrategy schemaInferenceStrategy) {
        super(
                startingCursorInitializer,
                stoppingCursorInitializer,
                shardAssigner,
                deserializationSchema,
                config);
        this.maxFetchLogGroups = maxFetchLogGroups;
        this.schemaInferenceStrategy = schemaInferenceStrategy;
    }

    public static PipelineSlsSourceBuilder builder() {
        return new PipelineSlsSourceBuilder();
    }

    @Override
    public SimpleVersionedSerializer<SlsShardSplit> getSplitSerializer() {
        return new PipelineSlsShardSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SlsEnumState> getEnumeratorCheckpointSerializer() {
        return new PipelineSlsSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<Event, SlsShardSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SlsSourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        SlsSourceReaderMetrics slsSourceReaderMetrics =
                new SlsSourceReaderMetrics(readerContext.metricGroup());
        SlsSourceFetcherManager fetcherManager =
                new SlsSourceFetcherManager(
                        elementsQueue,
                        () -> new SlsShardSplitReader(config, slsSourceReaderMetrics),
                        config);
        PipelineSlsRecordEmitter emitter = new PipelineSlsRecordEmitter(deserializationSchema);
        config.setBoolean(SourceReaderOptions.OVERRIDE_METRIC_NUM_RECORDS_IN, true);
        return new PipelineSlsSourceReader(
                elementsQueue, fetcherManager, emitter, config, readerContext);
    }

    @Override
    public SplitEnumerator<SlsShardSplit, SlsEnumState> createEnumerator(
            SplitEnumeratorContext<SlsShardSplit> enumContext) throws Exception {
        return new PipelineSlsSourceEnumerator(
                config,
                startingCursorInitializer,
                stoppingCursorInitializer,
                shardAssigner,
                enumContext,
                maxFetchLogGroups);
    }

    @Override
    public SplitEnumerator<SlsShardSplit, SlsEnumState> restoreEnumerator(
            SplitEnumeratorContext<SlsShardSplit> enumContext, SlsEnumState checkpoint)
            throws Exception {
        return new PipelineSlsSourceEnumerator(
                config,
                startingCursorInitializer,
                stoppingCursorInitializer,
                shardAssigner,
                enumContext,
                (PipelineSlsSourceEnumState) checkpoint,
                maxFetchLogGroups);
    }
}
