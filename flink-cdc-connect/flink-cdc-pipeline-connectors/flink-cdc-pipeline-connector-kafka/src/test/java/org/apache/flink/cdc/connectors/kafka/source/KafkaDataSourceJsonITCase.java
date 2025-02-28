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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.inference.SchemaInferenceSourceOptions;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.io.BaseEncoding;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link KafkaDataSource} with JSON format. */
public class KafkaDataSourceJsonITCase extends KafkaDataSourceITCaseBase {

    public static final List<Tuple2<String, String>> JSONS =
            Arrays.asList(
                    Tuple2.of("{\"id\":1}", "{\"name\":\"product1\", \"weight\":3.1}"),
                    Tuple2.of("{\"id\":2}", "{\"name\":\"product2\", \"weight\":3.2}"),
                    Tuple2.of("{\"id\":3}", "{\"name\":\"product3\", \"weight\":3.3}"));

    @Test
    @Timeout(120)
    public void testValueOnlyWithContinuousStrategy() throws Exception {
        prepareValues(JSONS.stream().map(record -> record.f1).collect(Collectors.toList()));
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.VALUE_FIELDS_PREFIX.key(), "value_");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");

        KafkaDataSourceFactory sourceFactory = new KafkaDataSourceFactory();
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                KafkaDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Thread.sleep(5_000);

        // produce new data with changed schema
        List<String> json =
                Arrays.asList(
                        "{\"name\":\"product10\", \"weight\":10, \"desc\":\"desc1\"}",
                        "{\"name\":\"product11\", \"weight\":\"11kg\", \"desc\":\"desc2\"}",
                        "{\"name\":\"product12\", \"weight\":12}");
        prepareValues(json);

        Thread.sleep(5_000);
        int expectedSize = 9;
        List<Event> actual = fetchResults(events, expectedSize);

        Schema expectedSchema = Schema.newBuilder().build();
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expected =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s, schema=columns={`value_name` STRING,`value_weight` DOUBLE}, primaryKeys=, options=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product1, 3.1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product2, 3.2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product3, 3.3], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`value_desc` STRING, position=AFTER, existedColumnName=value_weight}]}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product10, 10.0, desc1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "AlterColumnTypeEvent{tableId=%s, typeMapping={value_weight=STRING}, oldTypeMapping={value_weight=DOUBLE}}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product11, 11kg, desc2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product12, 12, null], op=INSERT, meta=()}",
                                topic));
        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testKeyValueWithContinuousStrategy() throws Exception {
        prepareRecords(JSONS);

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.KEY_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.KEY_FIELDS_PREFIX.key(), "key_");
        config.put(KafkaDataSourceOptions.VALUE_FIELDS_PREFIX.key(), "value_");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");

        KafkaDataSourceFactory sourceFactory = new KafkaDataSourceFactory();
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                KafkaDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Thread.sleep(5_000);

        // produce new data with changed schema
        List<Tuple2<String, String>> jsons =
                Arrays.asList(
                        Tuple2.of(
                                "{\"id\":10, \"other\":10}",
                                "{\"name\":\"product10\", \"weight\":10, \"desc\":\"desc1\"}"),
                        Tuple2.of(
                                "{\"id\":11, \"other\":11}",
                                "{\"name\":\"product11\", \"weight\":\"11kg\", \"desc\":\"desc2\"}"),
                        Tuple2.of(
                                "{\"id\":12, \"other\":12}",
                                "{\"name\":\"product12\", \"weight\":12}"));
        prepareRecords(jsons);

        Thread.sleep(5_000);
        int expectedSize = 9;
        List<Event> actual = fetchResults(events, expectedSize);

        Schema expectedSchema = Schema.newBuilder().build();
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expected =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s, schema=columns={`key_id` BIGINT,`value_name` STRING,`value_weight` DOUBLE}, primaryKeys=, options=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[1, product1, 3.1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[2, product2, 3.2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[3, product3, 3.3], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`value_desc` STRING, position=AFTER, existedColumnName=value_weight}]}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[10, product10, 10.0, desc1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "AlterColumnTypeEvent{tableId=%s, typeMapping={value_weight=STRING}, oldTypeMapping={value_weight=DOUBLE}}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[11, product11, 11kg, desc2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[12, product12, 12, null], op=INSERT, meta=()}",
                                topic));
        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testValueOnlyWithStaticStrategy() throws Exception {
        prepareValues(JSONS.stream().map(record -> record.f1).collect(Collectors.toList()));
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.VALUE_FIELDS_PREFIX.key(), "value_");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");

        KafkaDataSourceFactory sourceFactory = new KafkaDataSourceFactory();
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                KafkaDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Thread.sleep(5_000);

        // produce new data with changed schema
        List<String> json =
                Arrays.asList(
                        "{\"name\":\"product10\", \"weight\":10, \"desc\":\"desc1\"}",
                        "{\"name\":\"product11\", \"weight\":\"11.11\", \"desc\":\"desc2\"}",
                        "{\"name\":\"product12\", \"weight\":12.12}");
        prepareValues(json);

        Thread.sleep(5_000);
        int expectedSize = 7;
        List<Event> actual = fetchResults(events, expectedSize);

        Schema expectedSchema = Schema.newBuilder().build();
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expected =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s, schema=columns={`value_name` STRING,`value_weight` DOUBLE}, primaryKeys=, options=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product1, 3.1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product2, 3.2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product3, 3.3], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product10, 10.0], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product11, 11.11], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[product12, 12.12], op=INSERT, meta=()}",
                                topic));
        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testKeyValueWithStaticStrategy() throws Exception {
        prepareRecords(JSONS);
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.KEY_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.KEY_FIELDS_PREFIX.key(), "key_");
        config.put(KafkaDataSourceOptions.VALUE_FIELDS_PREFIX.key(), "value_");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");

        KafkaDataSourceFactory sourceFactory = new KafkaDataSourceFactory();
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                KafkaDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Thread.sleep(5_000);

        // produce new data with changed schema
        List<Tuple2<String, String>> jsons =
                Arrays.asList(
                        Tuple2.of(
                                "{\"id\":10, \"other\":10}",
                                "{\"name\":\"product10\", \"weight\":10, \"desc\":\"desc1\"}"),
                        Tuple2.of(
                                "{\"id\":11, \"other\":11}",
                                "{\"name\":\"product11\", \"weight\":\"11.11\", \"desc\":\"desc2\"}"),
                        Tuple2.of(
                                "{\"id\":12, \"other\":12}",
                                "{\"name\":\"product12\", \"weight\":12.12}"));
        prepareRecords(jsons);

        Thread.sleep(5_000);
        int expectedSize = 7;
        List<Event> actual = fetchResults(events, expectedSize);

        Schema expectedSchema = Schema.newBuilder().build();
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expected =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s, schema=columns={`key_id` BIGINT,`value_name` STRING,`value_weight` DOUBLE}, primaryKeys=, options=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[1, product1, 3.1], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[2, product2, 3.2], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[3, product3, 3.3], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[10, product10, 10.0], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[11, product11, 11.11], op=INSERT, meta=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[12, product12, 12.12], op=INSERT, meta=()}",
                                topic));
        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testHeadersMetadataColumn() throws Exception {
        List<Header> headers = new ArrayList<>();
        // Record without header
        prepareRecord(JSONS.get(0), headers);
        headers.clear();
        // Record with header
        headers.add(new RecordHeader("header_key_0", "header_value".getBytes()));
        headers.add(new RecordHeader("header_key_1", new byte[] {0, 1, 2, 3}));
        prepareRecord(JSONS.get(1), headers);

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.KEY_FORMAT.key(), "json");
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "json");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.METADATA_LIST.key(), "headers");

        KafkaDataSourceFactory sourceFactory = new KafkaDataSourceFactory();
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                KafkaDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        int expectedSize = 3;
        List<Event> actual = fetchResults(events, expectedSize);

        Schema expectedSchema = Schema.newBuilder().build();
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expected =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s, schema=columns={`id` BIGINT,`name` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[1, product1, 3.1], op=INSERT, meta=({headers={}})}",
                                topic),
                        String.format(
                                "DataChangeEvent{tableId=%s, before=[], after=[2, product2, 3.2], op=INSERT, meta=({headers={\"header_key_1\":\"%s\",\"header_key_0\":\"%s\"}})}",
                                topic,
                                BaseEncoding.base64().encode(new byte[] {0, 1, 2, 3}),
                                BaseEncoding.base64().encode("header_value".getBytes())));
        assertThat(actualReadableEvents).isEqualTo(expected);
    }
}
