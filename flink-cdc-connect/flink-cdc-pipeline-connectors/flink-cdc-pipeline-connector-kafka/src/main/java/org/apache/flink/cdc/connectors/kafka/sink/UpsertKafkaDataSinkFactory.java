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

package org.apache.flink.cdc.connectors.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.kafka.json.JsonFormatOptions;
import org.apache.flink.cdc.connectors.kafka.json.KeyJsonSerializationSchema;
import org.apache.flink.cdc.connectors.kafka.json.UpsertKafkaJsonSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.common.TimestampFormat;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.kafka.aliyun.AliyunKafkaClientParams.createAliyunKafkaClientParams;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.PROPERTIES_PREFIX;

/** A dummy {@link DataSinkFactory} to create {@link UpsertKafkaDataSink}. */
public class UpsertKafkaDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "upsert-kafka";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper helper = FactoryHelper.createFactoryHelper(this, context);
        // upsert kafka only supports json format now
        helper.validateExcept(PROPERTIES_PREFIX, "json");
        Configuration formatConfig = helper.getFormatConfig("json");

        DeliveryGuarantee deliveryGuarantee =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }

        final Properties kafkaProperties = new Properties();
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        allOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = allOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            kafkaProperties.put(subKey, value);
                        });
        String topic = context.getFactoryConfiguration().get(KafkaDataSinkOptions.TOPIC);
        boolean addTableToHeaderEnabled =
                context.getFactoryConfiguration()
                        .get(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED);
        String customHeaders =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.SINK_CUSTOM_HEADER);
        return new UpsertKafkaDataSink(
                deliveryGuarantee,
                kafkaProperties,
                createKeySerialization(formatConfig, zoneId),
                createValueSerialization(formatConfig, zoneId),
                topic,
                addTableToHeaderEnabled,
                customHeaders,
                createAliyunKafkaClientParams(context.getFactoryConfiguration()));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        options.add(KafkaDataSinkOptions.TOPIC);
        options.add(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED);

        options.add(KafkaDataSinkOptions.ALIYUN_KAFKA_AK);
        options.add(KafkaDataSinkOptions.ALIYUN_KAFKA_SK);
        options.add(KafkaDataSinkOptions.ALIYUN_KAFKA_INSTANCE_ID);
        options.add(KafkaDataSinkOptions.ALIYUN_KAFKA_ENDPOINT);
        options.add(KafkaDataSinkOptions.ALIYUN_KAFKA_REGION_ID);
        return options;
    }

    private static SerializationSchema<Event> createKeySerialization(
            Configuration formatOptions, ZoneId zoneId) {
        TimestampFormat timestampFormat = formatOptions.get(JsonFormatOptions.TIMESTAMP_FORMAT);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_MODE);
        String mapNullKeyLiteral = formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_LITERAL);

        final boolean encodeDecimalAsPlainNumber =
                formatOptions.get(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);

        final boolean writeNullProperties =
                formatOptions.get(JsonFormatOptions.WRITE_NULL_PROPERTIES);
        return new KeyJsonSerializationSchema(
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral,
                encodeDecimalAsPlainNumber,
                writeNullProperties,
                zoneId);
    }

    @VisibleForTesting
    public static SerializationSchema<Event> createValueSerialization(
            Configuration formatOptions, ZoneId zoneId) {
        TimestampFormat timestampFormat = formatOptions.get(JsonFormatOptions.TIMESTAMP_FORMAT);
        JsonFormatOptions.MapNullKeyMode mapNullKeyMode =
                formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_MODE);
        String mapNullKeyLiteral = formatOptions.get(JsonFormatOptions.MAP_NULL_KEY_LITERAL);
        boolean encodeDecimalAsPlainNumber =
                formatOptions.get(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        boolean writeNullProperties = formatOptions.get(JsonFormatOptions.WRITE_NULL_PROPERTIES);
        return new UpsertKafkaJsonSerializationSchema(
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral,
                encodeDecimalAsPlainNumber,
                writeNullProperties,
                zoneId);
    }
}
