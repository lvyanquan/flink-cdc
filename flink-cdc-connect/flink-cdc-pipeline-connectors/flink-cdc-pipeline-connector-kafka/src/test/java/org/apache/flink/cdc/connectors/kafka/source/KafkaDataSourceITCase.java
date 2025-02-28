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
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.inference.SchemaInferenceSourceOptions;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link KafkaDataSource}. */
public class KafkaDataSourceITCase extends KafkaDataSourceITCaseBase {

    @Test
    @Timeout(120)
    public void testDebeziumJsonFormat() throws Exception {
        prepareValues(readLines("debezium-data-schema-exclude.txt"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "0");

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

        int expectedSize = 17;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();
        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual).hasSize(17);
        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));

        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` BIGINT,`name` STRING,`description` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[101, scooter, Small 2-wheel scooter, 3.140000104904175], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[102, car battery, 12V car battery, 8.100000381469727], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.800000011920929], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[106, hammer, 16oz carpenter's hammer, 1.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[107, rocks, box of assorted rocks, 5.300000190734863], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[108, jacket, water resistent black wind breaker, 0.10000000149011612], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[109, spare tire, 24 inch spare tire, 22.200000762939453], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[106, hammer, 16oz carpenter's hammer, 1.0], after=[106, hammer, 18oz carpenter hammer, 1.0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[107, rocks, box of assorted rocks, 5.300000190734863], after=[107, rocks, box of assorted rocks, 5.099999904632568], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[110, jacket, water resistent white wind breaker, 0.20000000298023224], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[111, scooter, Big 2-wheel scooter , 5.179999828338623], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[110, jacket, water resistent white wind breaker, 0.20000000298023224], after=[110, jacket, new water resistent white wind breaker, 0.5], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[111, scooter, Big 2-wheel scooter , 5.179999828338623], after=[111, scooter, Big 2-wheel scooter , 5.170000076293945], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[111, scooter, Big 2-wheel scooter , 5.170000076293945], after=[], op=DELETE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testDebeziumJsonFormatPreFetch() throws Exception {
        prepareValues(readLines("debezium-data-schema-change.txt"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "debezium-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "5");

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

        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();

        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` BIGINT,`name` STRING,`description` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testDebeziumJsonStaticInferenceStrategy() throws Exception {
        // prepare data for initializing schema
        prepareValues(
                Arrays.asList(
                        "{\"before\":null,\"after\":{\"id\":999},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"c\"}",
                        "{\"before\":null,\"after\":{\"id\":1000,\"name\":\"test1000\",\"weight\":1.0},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"c\"}"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "debezium-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "latest-offset");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        // use the latest one record for parsing initial schema
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "1");

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

        Thread.sleep(10_000);
        prepareValues(readLines("debezium-data-schema-change.txt"));

        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();

        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));

        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` BIGINT,`name` STRING,`weight` DOUBLE}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3.0], after=[3, test3, 3.14], op=UPDATE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testDebeziumJsonContinuousInferenceStrategy() throws Exception {
        // prepare data for initializing schema
        prepareValues(
                Collections.singletonList(
                        "{\"before\":null,\"after\":{\"id\":999},\"source\":{\"db\":\"inventory\",\"table\":\"products\"},\"op\":\"c\"}"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "debezium-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "latest-offset");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "continuous");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "1");

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

        Thread.sleep(10_000);
        prepareValues(readLines("debezium-data-schema-change.txt"));

        int expectedSize = 10;
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
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` BIGINT}, primaryKeys=, options=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`name` STRING, position=AFTER, existedColumnName=id}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`description` STRING, position=AFTER, existedColumnName=name}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`weight` BIGINT, position=AFTER, existedColumnName=description}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=inventory.products, typeMapping={weight=DOUBLE}, oldTypeMapping={weight=BIGINT}}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testCanalJsonFormat() throws Exception {
        prepareValues(readLines("canal-data.txt"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "canal-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "0");

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

        int expectedSize = 21;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.STRING())
                        .physicalColumn("other", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        List<Event> actual = fetchResults(events, expectedSize);
        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products2, schema=columns={`id` STRING NOT NULL,`name` STRING,`description` STRING,`weight` STRING,`other` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[101, scooter, Small 2-wheel scooter, 3.14, val1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[102, car battery, 12V car battery, 8.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[104, hammer, 12oz carpenter's hammer, 0.75, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[105, hammer, 14oz carpenter's hammer, 0.875, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[106, hammer, null, 1.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[107, rocks, box of assorted rocks, 5.3, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[108, jacket, water resistent black wind breaker, 0.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[109, spare tire, 24 inch spare tire, 22.2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[106, hammer, null, 1.0, val2], after=[106, hammer, 18oz carpenter hammer, 1.0, val0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[107, rocks, box of assorted rocks, 5.3, null], after=[107, rocks, box of assorted rocks, 5.1, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[110, jacket, water resistent white wind breaker, 0.2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[], after=[111, scooter, Big 2-wheel scooter , 5.18, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[110, jacket, water resistent white wind breaker, 0.2, null], after=[110, jacket, new water resistent white wind breaker, 0.5, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.18, null], after=[111, scooter, Big 2-wheel scooter , 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[111, scooter, Big 2-wheel scooter , 5.17, null], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[101, scooter, Small 2-wheel scooter, 3.14, null], after=[101, scooter, Small 2-wheel scooter, 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 8.1, null], after=[102, car battery, 12V car battery, 5.17, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[102, car battery, 12V car battery, 5.17, null], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=inventory.products2, before=[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8, null], after=[], op=DELETE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testCanalJsonFormatPreFetch() throws Exception {
        prepareValues(readLines("canal-data-schema-change.txt"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "canal-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "5");

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

        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING NOT NULL,`name` STRING,`description` STRING,`weight` DOUBLE}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");
        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testCanalJsonStaticInferenceStrategy() throws Exception {
        // prepare data for initializing schema
        prepareValues(
                Arrays.asList(
                        "{\"data\":[{\"id\":\"999\"}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}",
                        "{\"data\":[{\"id\":\"1000\",\"name\":\"test\",\"weight\":1.0}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "canal-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "latest-offset");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        // use the latest one record for parsing initial schema
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "1");

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

        Thread.sleep(10_000);
        prepareValues(readLines("canal-data-schema-change.txt"));

        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));

        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING NOT NULL,`name` STRING,`weight` DOUBLE}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, 3.0], after=[3, test3, 3.14], op=UPDATE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testCanalJsonContinuousInferenceStrategy() throws Exception {
        // prepare data for initializing schema
        prepareValues(
                Collections.singletonList(
                        "{\"data\":[{\"id\":\"999\"}],\"database\":\"inventory\",\"table\":\"products\",\"old\":null,\"pkNames\":[\"id\"],\"type\":\"INSERT\"}"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "canal-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "latest-offset");
        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "continuous");
        config.put(KafkaDataSourceOptions.SCAN_MAX_PRE_FETCH_RECORDS.key(), "1");

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

        Thread.sleep(10_000);
        prepareValues(readLines("canal-data-schema-change.txt"));

        int expectedSize = 10;
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
                        "CreateTableEvent{tableId=inventory.products, schema=columns={`id` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`name` STRING, position=AFTER, existedColumnName=id}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`description` STRING, position=AFTER, existedColumnName=name}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=inventory.products, addedColumns=[ColumnWithPosition{column=`weight` BIGINT, position=AFTER, existedColumnName=description}]}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3], op=INSERT, meta=()}",
                        "AlterColumnTypeEvent{tableId=inventory.products, typeMapping={weight=DOUBLE}, oldTypeMapping={weight=BIGINT}}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");

        assertThat(actualReadableEvents).isEqualTo(expected);
    }

    @Test
    @Timeout(120)
    public void testMetadataColumn() throws Exception {
        prepareValues(readLines("canal-data-schema-change.txt"));

        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSourceOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        config.put(KafkaDataSourceOptions.TOPIC.key(), topic);
        config.put(KafkaDataSourceOptions.VALUE_FORMAT.key(), "canal-json");
        config.put(KafkaDataSourceOptions.SCAN_STARTUP_MODE.key(), "earliest-offset");
        config.put(KafkaDataSourceOptions.METADATA_LIST.key(), "topic,partition,offset,timestamp");

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

        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> actualReadableEvents = new ArrayList<>();
        actual.forEach(
                event ->
                        actualReadableEvents.add(
                                TestUtil.convertEventToStr(event, expectedSchema)));
        List<String> expected =
                Arrays.asList(
                        "CreateTableEvent\\{tableId=inventory\\.products, schema=columns=\\{`id` STRING NOT NULL,`name` STRING,`description` STRING,`weight` DOUBLE\\}, primaryKeys=id, options=\\(\\)\\}",
                        String.format(
                                "DataChangeEvent\\{tableId=inventory\\.products, before=\\[\\], after=\\[0, test0, null, null\\], op=INSERT, meta=\\(\\{topic=%s, partition=0, offset=0, timestamp=\\d+\\}\\)\\}",
                                topic),
                        String.format(
                                "DataChangeEvent\\{tableId=inventory\\.products, before=\\[\\], after=\\[1, test1, null, null\\], op=INSERT, meta=\\(\\{topic=%s, partition=0, offset=1, timestamp=\\d+\\}\\)\\}",
                                topic),
                        String.format(
                                "DataChangeEvent\\{tableId=inventory\\.products, before=\\[\\], after=\\[2, test2, description for test2, null\\], op=INSERT, meta=\\(\\{topic=%s, partition=0, offset=2, timestamp=\\d+\\}\\)\\}",
                                topic),
                        String.format(
                                "DataChangeEvent\\{tableId=inventory\\.products, before=\\[\\], after=\\[3, test3, description for test3, 3.0\\], op=INSERT, meta=\\(\\{topic=%s, partition=0, offset=3, timestamp=\\d+\\}\\)\\}",
                                topic),
                        String.format(
                                "DataChangeEvent\\{tableId=inventory\\.products, before=\\[3, test3, description for test3, 3.0\\], after=\\[3, test3, description for test3 update, 3.14\\], op=UPDATE, meta=\\(\\{topic=%s, partition=0, offset=4, timestamp=\\d+\\}\\)\\}",
                                topic));
        assertThat(equalsWithPattern(expected, actualReadableEvents)).isTrue();
    }
}
