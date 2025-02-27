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

import org.apache.flink.cdc.connectors.kafka.source.KafkaDataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

/** Defines the supported metadata columns for {@link KafkaDataSource}. */
public enum KafkaReadableMetadata {
    TOPIC(
            "topic",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public String read(ConsumerRecord<?, ?> record) {
                    return record.topic();
                }
            }),

    PARTITION(
            "partition",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Integer read(ConsumerRecord<?, ?> record) {
                    return record.partition();
                }
            }),

    OFFSET(
            "offset",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Long read(ConsumerRecord<?, ?> record) {
                    return record.offset();
                }
            }),

    TIMESTAMP(
            "timestamp",
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Long read(ConsumerRecord<?, ?> record) {
                    return record.timestamp();
                }
            });

    private final String key;

    private final MetadataConverter converter;

    KafkaReadableMetadata(String key, MetadataConverter converter) {
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

    /** Converter to read metadata from {@link ConsumerRecord}. */
    public interface MetadataConverter extends Serializable {
        Object read(ConsumerRecord<?, ?> record);
    }
}
