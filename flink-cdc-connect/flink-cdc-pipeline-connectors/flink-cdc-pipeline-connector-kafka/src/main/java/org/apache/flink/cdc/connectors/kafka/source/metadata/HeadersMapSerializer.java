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

import org.apache.flink.shaded.guava31.com.google.common.io.BaseEncoding;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/** Serialize and deserialize between Kafka headers to string. */
public class HeadersMapSerializer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String serialize(Map<String, byte[]> headers) {
        Map<String, String> stringMap = new HashMap<>();
        headers.forEach((k, v) -> stringMap.put(k, BaseEncoding.base64().encode(v)));

        try {
            return objectMapper.writeValueAsString(stringMap);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, byte[]> deserialize(String serialized) {
        Map<String, String> map;
        try {
            map = objectMapper.readValue(serialized, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        Map<String, byte[]> headers = new HashMap<>();
        map.forEach((k, v) -> headers.put(k, BaseEncoding.base64().decode(v)));
        return headers;
    }
}
