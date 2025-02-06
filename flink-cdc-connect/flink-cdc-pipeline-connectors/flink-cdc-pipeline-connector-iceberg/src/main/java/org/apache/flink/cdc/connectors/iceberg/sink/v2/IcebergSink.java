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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;

/** A {@link Sink} implementation for Apache Iceberg. */
public class IcebergSink
        implements Sink<Event>,
                SupportsPreWriteTopology<Event>,
                SupportsCommitter<WriteResultWrapper> {

    protected final Map<String, String> catalogOptions;

    private final ZoneId zoneId;

    public IcebergSink(
            Map<String, String> catalogOptions, String schemaOperatorUid, ZoneId zoneId) {
        this.catalogOptions = catalogOptions;
        this.zoneId = zoneId;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        return dataStream;
    }

    @Override
    public Committer<WriteResultWrapper> createCommitter(CommitterInitContext committerInitContext)
            throws IOException {
        return new IcebergCommitter(catalogOptions);
    }

    @Override
    public SimpleVersionedSerializer<WriteResultWrapper> getCommittableSerializer() {
        return new WriteResultWrapperSerializer();
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) throws IOException {
        return new IcebergWriter(
                catalogOptions,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber(),
                zoneId);
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) throws IOException {
        return new IcebergWriter(
                catalogOptions,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber(),
                zoneId);
    }
}
