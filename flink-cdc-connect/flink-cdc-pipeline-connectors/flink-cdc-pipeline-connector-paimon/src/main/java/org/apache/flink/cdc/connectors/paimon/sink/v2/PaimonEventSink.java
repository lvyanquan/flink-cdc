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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketAssignOperator;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketWrapper;
import org.apache.flink.cdc.connectors.paimon.sink.v2.bucket.BucketWrapperEventTypeInfo;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.time.ZoneId;

/** A {@link PaimonSink} to process {@link Event}. */
public class PaimonEventSink extends PaimonSink<Event> implements WithPreWriteTopology<Event> {

    public final String schemaOperatorUid;

    public final ZoneId zoneId;

    public PaimonEventSink(
            Options catalogOptions,
            String commitUser,
            PaimonRecordSerializer<Event> serializer,
            String schemaOperatorUid,
            ZoneId zoneId,
            ReadableConfig flinkConf) {
        super(catalogOptions, commitUser, serializer, flinkConf);
        this.schemaOperatorUid = schemaOperatorUid;
        this.zoneId = zoneId;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        // Shuffle by key hash => Assign bucket => Shuffle by bucket.
        return dataStream
                .transform(
                        "BucketAssign",
                        new BucketWrapperEventTypeInfo(),
                        new BucketAssignOperator(
                                catalogOptions, schemaOperatorUid, zoneId, commitUser, flinkConf))
                .name("Assign Bucket")
                // All Events after BucketAssignOperator are decorated with BucketWrapper.
                .partitionCustom(
                        (bucket, numPartitions) -> bucket % numPartitions,
                        (event) -> ((BucketWrapper) event).getBucket());
    }

    @Override
    public SimpleVersionedSerializer<MultiTableCommittable> getCommittableSerializer() {
        CommitMessageSerializer fileSerializer = new CommitMessageSerializer();
        return new MultiTableCommittableSerializer(fileSerializer);
    }
}
