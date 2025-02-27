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

package org.apache.flink.cdc.connectors.sls.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogGroupSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.schema.SchemaAware;
import org.apache.flink.cdc.connectors.sls.source.split.PipelineSlsShardSplitState;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.ververica.connectors.sls.newsource.model.SlsSourceRecord;
import com.alibaba.ververica.connectors.sls.newsource.reader.SlsRecordEmitter;
import com.alibaba.ververica.connectors.sls.newsource.reader.format.SlsLogGroupDeserializationSchema;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplitState;
import com.aliyun.openservices.log.common.FastLogGroup;

import java.io.IOException;

/**
 * The {@link RecordEmitter} implementation for {@link PipelineSlsSourceReader}. Compared to {@link
 * SlsRecordEmitter}, it updates table schema in {@link PipelineSlsShardSplitState} additionally.
 */
public class PipelineSlsRecordEmitter
        implements RecordEmitter<SlsSourceRecord, Event, SlsShardSplitState>, SchemaAware {

    private final SlsLogGroupSchemaAwareDeserializationSchema<Event> deserializationSchema;
    private final SourceOutputWrapper sourceOutputWrapper = new SourceOutputWrapper();

    public PipelineSlsRecordEmitter(SlsLogGroupDeserializationSchema<Event> deserializationSchema) {
        this.deserializationSchema =
                (SlsLogGroupSchemaAwareDeserializationSchema<Event>) deserializationSchema;
    }

    @Override
    public void emitRecord(
            SlsSourceRecord slsSourceRecord,
            SourceOutput<Event> output,
            SlsShardSplitState splitState)
            throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(slsSourceRecord.getCursorTime());
            TableId tableId =
                    TableId.tableId(
                            splitState.getLogstore().getProject(),
                            splitState.getLogstore().getLogstore());
            for (FastLogGroup logGroup : slsSourceRecord.getContent()) {
                this.deserializationSchema.deserialize(logGroup, tableId, sourceOutputWrapper);
            }
            splitState.setCurrentCursor(slsSourceRecord.getNextCursor());

            Schema schema = deserializationSchema.getTableSchema(tableId);
            if (schema == null) {
                throw new FlinkRuntimeException(
                        String.format("Cannot retrieve schema state for table [%s]", tableId));
            }
            ((PipelineSlsShardSplitState) splitState).setTableSchema(tableId, schema);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize SLS source records due to", e);
        }
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return deserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        deserializationSchema.setTableSchema(tableId, schema);
    }

    private static class SourceOutputWrapper implements Collector<Event> {

        private SourceOutput<Event> sourceOutput;
        private long timestamp;

        @Override
        public void collect(Event record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {}

        public void setSourceOutput(SourceOutput<Event> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
