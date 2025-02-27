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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.connectors.sls.source.metadata.SlsReadableMetadata;
import org.apache.flink.cdc.connectors.sls.source.metadata.SourceMetadataColumn;
import org.apache.flink.cdc.connectors.sls.source.metadata.TagMetadataColumn;
import org.apache.flink.cdc.connectors.sls.source.metadata.TimestampMetadataColumn;
import org.apache.flink.cdc.connectors.sls.source.metadata.TopicMetadataColumn;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsEventDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogContinuousDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogGroupSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogStaticDeserializationSchema;

import com.alibaba.ververica.connectors.sls.SLSAccessInfo;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.assigner.ShardAssigner;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.initializer.CursorInitializer;
import com.alibaba.ververica.connectors.sls.source.StartupMode;

import java.util.List;

/** A {@link DataSource} for "SLS" connector. */
public class SlsDataSource implements DataSource {

    private final SLSAccessInfo accessInfo;
    private final int startInSec;
    private final int stopInSec;
    private final boolean exitAfterFinish;
    private final long shardDiscoveryIntervalMs;
    private final SchemaInferenceStrategy schemaInferenceStrategy;
    private final int maxFetchRecords;
    private final List<SlsReadableMetadata> readableMetadataList;

    public SlsDataSource(
            SLSAccessInfo accessInfo,
            int startInSec,
            int stopInSec,
            boolean exitAfterFinish,
            long shardDiscoveryIntervalMs,
            SchemaInferenceStrategy schemaInferenceStrategy,
            int maxFetchRecords,
            List<SlsReadableMetadata> readableMetadataList) {
        this.accessInfo = accessInfo;
        this.startInSec = startInSec;
        this.stopInSec = stopInSec;
        this.exitAfterFinish = exitAfterFinish;
        this.shardDiscoveryIntervalMs = shardDiscoveryIntervalMs;
        this.schemaInferenceStrategy = schemaInferenceStrategy;
        this.maxFetchRecords = maxFetchRecords;
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        SlsLogGroupSchemaAwareDeserializationSchema<Event> logGroupDeserializationSchema =
                getDeserializationSchema();

        CursorInitializer startingInitializer =
                getCursorInitializer(accessInfo.getStartupMode(), startInSec);

        PipelineSlsSourceBuilder builder = PipelineSlsSource.builder();
        builder.endpoint(accessInfo.getEndpoint())
                .project(accessInfo.getProjectName())
                .logstore(accessInfo.getLogstore())
                .accessId(accessInfo.getAccessId())
                .accessKey(accessInfo.getAccessKey())
                .consumerGroupId(accessInfo.getConsumerGroup())
                .query(accessInfo.getQuery())
                .maxRetries(accessInfo.getMaxRetries())
                .batchGetSize(accessInfo.getBatchGetSize())
                .compressType(accessInfo.getCompressType())
                .regionId(accessInfo.getRegionId())
                .signVersion(accessInfo.getSignVersion())
                .shardModDivisor(accessInfo.getShardModDivisor())
                .shardModRemainder(accessInfo.getShardModRemainder())
                .shardAssigner(ShardAssigner.roundRobin())
                .startingOffsets(startingInitializer)
                .stoppingOffsets(stopInSec < 0 ? null : stopInSec)
                .exitAfterReadFinish(exitAfterFinish)
                .shardDiscoveryIntervalMs(shardDiscoveryIntervalMs);

        builder.deserializer(logGroupDeserializationSchema)
                .schemaInferenceStrategy(schemaInferenceStrategy)
                .maxFetchRecords(maxFetchRecords);

        return FlinkSourceProvider.of(builder.build());
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        // this method is never used now
        throw new UnsupportedOperationException(
                "SLS data source does not support getMetadataAccessor now.");
    }

    @Override
    public boolean isParallelMetadataSource() {
        return true;
    }

    @Override
    public SupportedMetadataColumn[] supportedMetadataColumns() {
        return new SupportedMetadataColumn[] {
            new SourceMetadataColumn(),
            new TopicMetadataColumn(),
            new TimestampMetadataColumn(),
            new TagMetadataColumn()
        };
    }

    private CursorInitializer getCursorInitializer(StartupMode mode, int timeSec) {
        switch (mode) {
            case EARLIEST:
                return CursorInitializer.earliest();
            case TIMESTAMP:
                return CursorInitializer.timestamp(timeSec);
            case LATEST:
            default:
                return CursorInitializer.latest();
        }
    }

    private SlsLogGroupSchemaAwareDeserializationSchema<Event> getDeserializationSchema() {
        SlsLogSchemaAwareDeserializationSchema<Event> logDeserializationSchema;
        switch (schemaInferenceStrategy) {
            case CONTINUOUS:
                logDeserializationSchema = new SlsLogContinuousDeserializationSchema();
                break;
            case STATIC:
                logDeserializationSchema = new SlsLogStaticDeserializationSchema();
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown schema inference strategy: %s", schemaInferenceStrategy));
        }

        return new SlsEventDeserializationSchema(logDeserializationSchema, readableMetadataList);
    }

    @VisibleForTesting
    SLSAccessInfo getAccessInfo() {
        return accessInfo;
    }

    @VisibleForTesting
    public int getStartInSec() {
        return startInSec;
    }
}
