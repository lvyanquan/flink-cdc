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

package org.apache.flink.cdc.connectors.sls.source;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.inference.SchemaInferenceStrategy;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogGroupSchemaAwareDeserializationSchema;
import org.apache.flink.cdc.connectors.sls.source.reader.deserializer.SlsLogSchemaAwareDeserializationSchema;

import com.alibaba.ververica.connectors.sls.newsource.SlsSource;
import com.alibaba.ververica.connectors.sls.newsource.SlsSourceBuilder;
import com.alibaba.ververica.connectors.sls.newsource.reader.format.SlsLogDeserializationSchema;
import com.alibaba.ververica.connectors.sls.newsource.reader.format.SlsLogGroupDeserializationSchema;

/** The builder class for {@link PipelineSlsSource}. */
public class PipelineSlsSourceBuilder extends SlsSourceBuilder<Event> {

    private SchemaInferenceStrategy schemaInferenceStrategy;
    private int maxFetchRecords;

    public PipelineSlsSourceBuilder() {
        super();
    }

    public PipelineSlsSourceBuilder schemaInferenceStrategy(
            SchemaInferenceStrategy schemaInferenceStrategy) {
        this.schemaInferenceStrategy = schemaInferenceStrategy;
        return this;
    }

    public PipelineSlsSourceBuilder maxFetchRecords(int maxFetchRecords) {
        this.maxFetchRecords = maxFetchRecords;
        return this;
    }

    @Override
    public PipelineSlsSourceBuilder deserializer(
            SlsLogGroupDeserializationSchema<Event> deserializationSchema) {
        if (!(deserializationSchema instanceof SlsLogGroupSchemaAwareDeserializationSchema)) {
            throw new IllegalArgumentException(
                    "DeserializationSchema should be an instance of SlsLogGroupSchemaAwareDeserializationSchema or SlsLogSchemaAwareDeserializationSchema");
        }
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    @Override
    public PipelineSlsSourceBuilder deserializer(
            SlsLogDeserializationSchema<Event> deserializationSchema) {
        if (!(deserializationSchema instanceof SlsLogSchemaAwareDeserializationSchema)) {
            throw new IllegalArgumentException(
                    "DeserializationSchema should be an instance of SlsLogGroupSchemaAwareDeserializationSchema or SlsLogSchemaAwareDeserializationSchema");
        }
        this.deserializationSchema =
                SlsLogGroupSchemaAwareDeserializationSchema.logOnly(
                        (SlsLogSchemaAwareDeserializationSchema<Event>) deserializationSchema);
        return this;
    }

    @Override
    public SlsSource<Event> build() {
        sanityCheck();
        return new PipelineSlsSource(
                startingCursorsInitializer,
                stoppingOffsetsInitializer,
                shardAssigner,
                deserializationSchema,
                config,
                maxFetchRecords,
                schemaInferenceStrategy);
    }
}
