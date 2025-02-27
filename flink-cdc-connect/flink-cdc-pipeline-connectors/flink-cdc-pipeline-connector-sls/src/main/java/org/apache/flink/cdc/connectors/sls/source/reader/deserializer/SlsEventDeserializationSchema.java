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

package org.apache.flink.cdc.connectors.sls.source.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.sls.source.SlsDataSource;
import org.apache.flink.cdc.connectors.sls.source.metadata.SlsReadableMetadata;
import org.apache.flink.cdc.connectors.sls.source.util.StringMapSerializer;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A specific {@link SlsLogGroupSchemaAwareDeserializationSchema} for {@link SlsDataSource}. */
public class SlsEventDeserializationSchema
        implements SlsLogGroupSchemaAwareDeserializationSchema<Event> {

    private final SlsLogSchemaAwareDeserializationSchema<Event> logDeserializationSchema;

    private final OutputCollector outputCollector;

    public SlsEventDeserializationSchema(
            SlsLogSchemaAwareDeserializationSchema<Event> logDeserializationSchema,
            List<SlsReadableMetadata> metadataList) {
        this.logDeserializationSchema = logDeserializationSchema;

        this.outputCollector = new OutputCollector(metadataList);
    }

    @Override
    public void deserialize(FastLogGroup fastLogGroup, Collector<Event> collector)
            throws IOException {
        throw new RuntimeException(
                "Please invoke SlsLogGroupDeserializationSchema#deserialize(byte[], TableId, Collector<Event>) instead.");
    }

    @Override
    public void deserialize(FastLogGroup fastLogGroup, TableId tableId, Collector<Event> collector)
            throws IOException {
        outputCollector.collector = collector;
        outputCollector.fastLogGroup = fastLogGroup;
        for (FastLog log : fastLogGroup.getLogs()) {
            outputCollector.fastLog = log;
            logDeserializationSchema.deserialize(log, tableId, outputCollector);
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return logDeserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        logDeserializationSchema.setTableSchema(tableId, schema);
    }

    // --------------------------------------------------------------------------------------------

    private static final class OutputCollector implements Collector<Event>, Serializable {
        private static final long serialVersionUID = 1L;
        private final List<SlsReadableMetadata> readableMetadataList;

        private transient FastLogGroup fastLogGroup;
        private transient FastLog fastLog;
        private transient Collector<Event> collector;

        OutputCollector(List<SlsReadableMetadata> readableMetadataList) {
            this.readableMetadataList = readableMetadataList;
        }

        @Override
        public void collect(Event event) {
            if (!readableMetadataList.isEmpty() && event instanceof DataChangeEvent) {
                DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                Map<String, String> meta = new HashMap<>(dataChangeEvent.meta());
                readableMetadataList.forEach(
                        readableMetadata -> {
                            Object metadata =
                                    readableMetadata.getConverter().read(fastLogGroup, fastLog);
                            if (SlsReadableMetadata.TAG.equals(readableMetadata)) {
                                meta.put(
                                        readableMetadata.getKey(),
                                        StringMapSerializer.serialize(
                                                (Map<String, String>) metadata));
                            } else {
                                meta.put(readableMetadata.getKey(), String.valueOf(metadata));
                            }
                        });
                collector.collect(DataChangeEvent.replaceMeta(dataChangeEvent, meta));
            } else {
                collector.collect(event);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }
    }
}
