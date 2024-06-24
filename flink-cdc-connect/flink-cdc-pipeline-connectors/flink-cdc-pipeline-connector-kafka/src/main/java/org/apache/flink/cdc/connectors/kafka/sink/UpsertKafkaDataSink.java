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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkDataStreamSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafka.aliyun.AliyunKafkaClientParams;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;

import java.util.Properties;

import static org.apache.flink.cdc.common.event.DataChangeEvent.deleteEvent;
import static org.apache.flink.cdc.common.event.DataChangeEvent.insertEvent;

/** A {@link DataSink} for "Kafka" connector. */
public class UpsertKafkaDataSink implements DataSink {

    final Properties kafkaProperties;

    final DeliveryGuarantee deliveryGuarantee;

    final SerializationSchema<Event> valueSerialization;
    final SerializationSchema<Event> keySerialization;

    final String topic;

    final String customHeaders;

    final boolean addTableToHeaderEnabled;

    @Nullable private final AliyunKafkaClientParams aliyunKafkaParams;

    public UpsertKafkaDataSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProperties,
            SerializationSchema<Event> keySerialization,
            SerializationSchema<Event> valueSerialization,
            String topic,
            boolean addTableToHeaderEnabled,
            String customHeaders,
            @Nullable AliyunKafkaClientParams aliyunKafkaParams) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProperties = kafkaProperties;
        this.keySerialization = keySerialization;
        this.valueSerialization = valueSerialization;
        this.topic = topic;
        this.addTableToHeaderEnabled = addTableToHeaderEnabled;
        this.customHeaders = customHeaders;
        this.aliyunKafkaParams = aliyunKafkaParams;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        final KafkaSinkBuilder<Event> sinkBuilder = KafkaSink.builder();
        return new FlinkDataStreamSinkProvider() {
            @Override
            public Tuple2<DataStream<Event>, Sink<Event>> consumeDataStream(
                    DataStream<Event> dataStream) {
                return Tuple2.of(
                        dataStream.flatMap(
                                new FlatMapFunction<Event, Event>() {
                                    @Override
                                    public void flatMap(Event event, Collector<Event> collector)
                                            throws Exception {
                                        if (event instanceof DataChangeEvent) {
                                            DataChangeEvent dataChangeEvent =
                                                    (DataChangeEvent) event;
                                            if (dataChangeEvent.op() == OperationType.UPDATE) {
                                                // Split update to delete and insert to make sure
                                                // the upsert kafka will update this record
                                                collector.collect(
                                                        deleteEvent(
                                                                dataChangeEvent.tableId(),
                                                                dataChangeEvent.before(),
                                                                dataChangeEvent.meta()));
                                                collector.collect(
                                                        insertEvent(
                                                                dataChangeEvent.tableId(),
                                                                dataChangeEvent.after(),
                                                                dataChangeEvent.meta()));
                                                return;
                                            } else if (dataChangeEvent.op()
                                                    == OperationType.REPLACE) {
                                                throw new UnsupportedOperationException(
                                                        "Upsert kafka does not support replace type.");
                                            }
                                        }
                                        collector.collect(event);
                                    }
                                }),
                        sinkBuilder
                                .setDeliveryGuarantee(deliveryGuarantee)
                                .setBootstrapServers(
                                        kafkaProperties
                                                .get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
                                                .toString())
                                .setKafkaProducerConfig(kafkaProperties)
                                .setRecordSerializer(
                                        new PipelineUpsertKafkaRecordSerializationSchema(
                                                keySerialization,
                                                valueSerialization,
                                                topic,
                                                addTableToHeaderEnabled,
                                                customHeaders))
                                .build());
            }
        };
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new KafkaMetadataApplier(true, kafkaProperties, aliyunKafkaParams);
    }
}
