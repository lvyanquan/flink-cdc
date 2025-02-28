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

package org.apache.flink.cdc.connectors.kafka.source.metadata;

import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.table.data.GenericMapData;

import java.util.HashMap;
import java.util.Map;

/** A {@link SupportedMetadataColumn} for headers. */
public class HeadersMetadataColumn implements SupportedMetadataColumn {
    @Override
    public String getName() {
        return "headers";
    }

    @Override
    public DataType getType() {
        return DataTypes.MAP(DataTypes.STRING(), DataTypes.BYTES());
    }

    @Override
    public Class<?> getJavaClass() {
        return MapData.class;
    }

    @Override
    public Object read(Map<String, String> metadata) {
        if (metadata.containsKey(getName())) {
            Map<String, byte[]> tags = HeadersMapSerializer.deserialize(metadata.get(getName()));
            Map<StringData, byte[]> map = new HashMap<>();
            tags.forEach((k, v) -> map.put(BinaryStringData.fromString(k), v));
            return new GenericMapData(map);
        }
        throw new IllegalArgumentException(
                String.format("%s doesn't exist in the metadata: %s", getName(), metadata));
    }
}
