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

package org.apache.flink.cdc.connectors.hologres.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.hologres.sink.v2.api.AbstractHologresWriter;
import org.apache.flink.cdc.connectors.hologres.sink.v2.api.HologresJDBCWriter;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.sink.v2.events.HologresSchemaFlushRecord;
import org.apache.flink.metrics.Counter;

import com.alibaba.hologres.client.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A {@link SinkWriter} for Hologres. */
@Internal
public class HologresSinkWriter<INPUT> implements SinkWriter<INPUT> {
    private static final transient Logger LOG = LoggerFactory.getLogger(HologresSinkWriter.class);

    private HologresRecordSerializer<INPUT> serializer;

    private AbstractHologresWriter hologresWriter;

    private final HologresConnectionParam param;

    private final Counter numRecordsOutCounter;
    private final Counter numBytesSendCounter;

    public HologresSinkWriter(
            HologresRecordSerializer<INPUT> serializer,
            HologresConnectionParam param,
            Sink.InitContext initContext) {
        this.serializer = serializer;
        this.param = param;
        this.numRecordsOutCounter = initContext.metricGroup().getNumRecordsSendCounter();
        this.numBytesSendCounter = initContext.metricGroup().getNumBytesSendCounter();
    }

    @Override
    public void write(INPUT input, Context context) throws IOException, InterruptedException {
        Record record = serializer.serialize(input);
        if (hologresWriter == null) {
            hologresWriter = getHologresWriter();
        }
        if (record instanceof HologresSchemaFlushRecord) {
            hologresWriter.flush();
        } else if (record != null) {
            long byteSize = hologresWriter.write(record);
            numRecordsOutCounter.inc();
            numBytesSendCounter.inc(byteSize);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (hologresWriter != null) {
            hologresWriter.flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (hologresWriter != null) {
            hologresWriter.flush();
            hologresWriter.close();
            hologresWriter = null;
        }
    }

    private AbstractHologresWriter getHologresWriter() throws IOException {
        if (hologresWriter == null) {
            hologresWriter = new HologresJDBCWriter(param);
            hologresWriter.open();
        }
        return hologresWriter;
    }
}
