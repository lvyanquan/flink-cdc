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

package org.apache.flink.cdc.connectors.hologres.schema;

import org.apache.flink.cdc.connectors.hologres.HologresJDBCClientProvider;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.Serializable;
import java.util.Optional;

/** A class wrapper of Hologres TableSchema class to distinguish with Flink's TableSchema class. */
public class HologresTableSchema implements Serializable {
    private final TableSchema hologresSchema;

    public HologresTableSchema(TableSchema hologresSchema) {
        this.hologresSchema = hologresSchema;
    }

    public TableSchema get() {
        return this.hologresSchema;
    }

    public static HologresTableSchema get(
            HologresConnectionParam param, String qualifiedTableName) {
        HologresJDBCClientProvider provider = new HologresJDBCClientProvider(param.getHoloConfig());
        try (HoloClient client = provider.getClient()) {
            TableSchema tableSchema = client.getTableSchema(qualifiedTableName, true);
            return new HologresTableSchema(tableSchema);
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static HologresTableSchema get(HoloClient client, String qualifiedTableName) {
        try {
            TableSchema tableSchema = client.getTableSchema(qualifiedTableName, true);
            return new HologresTableSchema(tableSchema);
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<Column> getColumn(String columnName) {
        Integer index = hologresSchema.getColumnIndex(columnName);
        if (index == null || index < 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(hologresSchema.getColumn(index));
    }

    public String getPartitionColumnName() {
        return hologresSchema.getPartitionInfo();
    }

    public boolean isPartitionParentTable() {
        return hologresSchema.isPartitionParentTable();
    }
}
