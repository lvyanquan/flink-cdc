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

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.connectors.kafka.source.reader.deserializer.KafkaRecordSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.kafka.source.schema.RecordSchemaParser;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

/** The builder class for {@link PipelineKafkaSource}. */
public class PipelineKafkaSourceBuilder extends KafkaSourceBuilder<Event> {

    private SchemaInferenceStrategy schemaInferenceStrategy;
    private RecordSchemaParser keyRecordSchemaParser;
    private RecordSchemaParser valueRecordSchemaParser;
    private int maxFetchRecords;

    public PipelineKafkaSourceBuilder() {
        super();
    }

    public PipelineKafkaSourceBuilder setSchemaInferenceStrategy(
            SchemaInferenceStrategy schemaInferenceStrategy) {
        this.schemaInferenceStrategy = schemaInferenceStrategy;
        return this;
    }

    public PipelineKafkaSourceBuilder setValueRecordSchemaParser(
            RecordSchemaParser valueRecordSchemaParser) {
        this.valueRecordSchemaParser = valueRecordSchemaParser;
        return this;
    }

    public PipelineKafkaSourceBuilder setKeyRecordSchemaParser(
            RecordSchemaParser keyRecordSchemaParser) {
        this.keyRecordSchemaParser = keyRecordSchemaParser;
        return this;
    }

    public PipelineKafkaSourceBuilder setMaxFetchRecords(int maxFetchRecords) {
        this.maxFetchRecords = maxFetchRecords;
        return this;
    }

    @Override
    public PipelineKafkaSourceBuilder setDeserializer(
            KafkaRecordDeserializationSchema<Event> deserializationSchema) {
        if (!(deserializationSchema instanceof KafkaRecordSchemaAwareDeserializationSchema)) {
            throw new IllegalArgumentException(
                    "DeserializationSchema should implement KafkaRecordSchemaAwareDeserializationSchema.");
        }
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    @Override
    public KafkaSource<Event> build() {
        sanityCheck();
        parseAndSetRequiredProperties();
        return new PipelineKafkaSource(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                deserializationSchema,
                props,
                schemaInferenceStrategy,
                keyRecordSchemaParser,
                valueRecordSchemaParser,
                maxFetchRecords);
    }
}
