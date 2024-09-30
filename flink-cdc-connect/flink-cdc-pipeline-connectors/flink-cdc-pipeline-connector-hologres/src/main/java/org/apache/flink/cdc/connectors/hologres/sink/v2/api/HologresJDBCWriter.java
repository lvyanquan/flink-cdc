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

package org.apache.flink.cdc.connectors.hologres.sink.v2.api;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.hologres.HologresJDBCClientProvider;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Hologres writer base on jdbc. */
@Internal
public class HologresJDBCWriter implements AbstractHologresWriter {
    private static final transient Logger LOG = LoggerFactory.getLogger(HologresJDBCWriter.class);

    private final HologresConnectionParam param;
    private transient HologresJDBCClientProvider hologresJDBCClientProvider;

    public HologresJDBCWriter(HologresConnectionParam param) {
        this.param = param;
    }

    @Override
    public void open() throws IOException {
        LOG.info(
                "Initiating {} for database: {}, the whole configs is {}",
                getClass().getSimpleName(),
                param.getDatabase(),
                param);
        hologresJDBCClientProvider =
                new HologresJDBCClientProvider(
                        param.getHoloConfig(), param.getJdbcSharedConnectionPoolName());
    }

    @Override
    public long write(Record record) throws IOException {
        LOG.debug("Hologres insert or delete record in JDBC: {}", record);
        try {
            hologresJDBCClientProvider.getClient().put(new Put(record));
        } catch (HoloClientException e) {
            throw new IOException(
                    String.format("Hologres fail to insert or delete record %s ", record), e);
        }

        return record.getByteSize();
    }

    @Override
    public void flush() throws IOException {
        try {
            hologresJDBCClientProvider.getClient().flush();
        } catch (HoloClientException e) {
            throw new IOException("Hologres fail to flush records.", e);
        }
    }

    @Override
    public void close() throws IOException {
        hologresJDBCClientProvider.closeClient();
    }
}
