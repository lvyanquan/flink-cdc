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

package org.apache.flink.cdc.connectors.sls.source.metadata;

import org.apache.flink.cdc.connectors.sls.source.SlsDataSource;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.stream.Collectors;

/** Defines the supported metadata columns for {@link SlsDataSource}. */
public enum SlsReadableMetadata {
    SOURCE(
            "__source__",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(FastLogGroup fastLogGroup, FastLog fastLog) {
                    return fastLogGroup.getSource();
                }
            }),
    TOPIC(
            "__topic__",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(FastLogGroup fastLogGroup, FastLog fastLog) {
                    return fastLogGroup.getTopic();
                }
            }),
    TIMESTAMP(
            "__timestamp__",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(FastLogGroup fastLogGroup, FastLog fastLog) {
                    return (long) fastLog.getTime();
                }
            }),
    TAG(
            "__tag__",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(FastLogGroup fastLogGroup, FastLog fastLog) {
                    return new HashMap<>(
                            fastLogGroup.getTags().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    FastLogTag::getKey, FastLogTag::getValue)));
                }
            }),
    ;

    private final String key;
    private final MetadataConverter converter;

    SlsReadableMetadata(String key, MetadataConverter converter) {
        this.key = key;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public MetadataConverter getConverter() {
        return converter;
    }

    // --------------------------------------------------------------------------------------------

    /** Converter to read metadata. */
    public interface MetadataConverter extends Serializable {
        Object read(FastLogGroup fastLogGroup, FastLog fastLog);
    }
}
