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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafka.KafkaUtil;
import org.apache.flink.cdc.connectors.kafka.TestUtil;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link KafkaDataSource}. */
public class KafkaDataSourceITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataSourceITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static AdminClient admin;
    private static final String KAFKA = "reg.docker.alibaba-inc.com/confluentinc/cp-kafka:7.2.2";
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private String topic;
    private KafkaProducer<String, String> producer;

    public static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeAll
    public static void setupAdmin() {
        KAFKA_CONTAINER.start();
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
    }

    @AfterAll
    public static void afterAll() {
        admin.close();
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        topic = "test-topic-" + UUID.randomUUID();
        TestValuesTableFactory.clearAllData();
        env.setParallelism(2);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
        producer =
                new KafkaProducer<>(
                        getKafkaClientConfiguration(),
                        new StringSerializer(),
                        new StringSerializer());
    }

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        deleteTestTopic(topic);
    }

    @Test
    @Timeout(120)
    public void testDebeziumJsonFormat() throws Exception {
        prepareData(readLines("debezium-data-schema-exclude.txt"));

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

        Thread.sleep(10_000);

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

        assertThat(actual.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent1 = (DataChangeEvent) actual.get(1);
        assertThat(dataChangeEvent1.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent1.after();
        assertThat(data.getLong(0)).isEqualTo(101L);
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Small 2-wheel scooter");
        assertThat(data.getDouble(3)).isEqualTo(3.140000104904175);

        assertThat(actual.get(10)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent2 = (DataChangeEvent) actual.get(10);
        assertThat(dataChangeEvent2.op()).isEqualTo(OperationType.UPDATE);
        RecordData before = dataChangeEvent2.before();
        RecordData after = dataChangeEvent2.after();
        assertThat(before.getLong(0)).isEqualTo(106L);
        assertThat(before.getString(1).toString()).isEqualTo("hammer");
        assertThat(before.getString(2).toString()).isEqualTo("16oz carpenter's hammer");
        assertThat(before.getDouble(3)).isEqualTo(1d);
        assertThat(after.getLong(0)).isEqualTo(106L);
        assertThat(after.getString(1).toString()).isEqualTo("hammer");
        assertThat(after.getString(2).toString()).isEqualTo("18oz carpenter hammer");
        assertThat(after.getDouble(3)).isEqualTo(1d);
    }

    @Test
    @Timeout(120)
    public void testDebeziumJsonFormatPreFetch() throws Exception {
        prepareData(readLines("debezium-data-schema-change.txt"));

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

        Thread.sleep(10_000);
        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .build();
        List<RecordData.FieldGetter> fieldGetters =
                TestUtil.getFieldGettersBySchema(expectedSchema);

        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> expectedDataChangeEvents =
                Arrays.asList(
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");
        for (int i = 1; i < expectedSize; i++) {
            assertThat(actual.get(i)).isInstanceOf(DataChangeEvent.class);
            assertThat(TestUtil.convertEventToStr(actual.get(i), fieldGetters))
                    .isEqualTo(expectedDataChangeEvents.get(i - 1));
        }
    }

    @Test
    @Timeout(120)
    public void testCanalJsonFormat() throws Exception {
        prepareData(readLines("canal-data.txt"));

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

        Thread.sleep(10_000);
        int expectedSize = 21;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
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

        assertThat(actual.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent1 = (DataChangeEvent) actual.get(1);
        assertThat(dataChangeEvent1.op()).isEqualTo(OperationType.INSERT);
        RecordData data = dataChangeEvent1.after();
        assertThat(data.getString(0).toString()).isEqualTo("101");
        assertThat(data.getString(1).toString()).isEqualTo("scooter");
        assertThat(data.getString(2).toString()).isEqualTo("Small 2-wheel scooter");
        assertThat(data.getString(3).toString()).isEqualTo("3.14");
        assertThat(data.getString(4).toString()).isEqualTo("val1");

        assertThat(actual.get(10)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent dataChangeEvent2 = (DataChangeEvent) actual.get(10);
        assertThat(dataChangeEvent2.op()).isEqualTo(OperationType.UPDATE);
        RecordData before = dataChangeEvent2.before();
        RecordData after = dataChangeEvent2.after();
        assertThat(before.getString(0).toString()).isEqualTo("106");
        assertThat(before.getString(1).toString()).isEqualTo("hammer");
        assertThat(before.getString(2).toString()).isEmpty();
        assertThat(before.getString(3).toString()).isEqualTo("1.0");
        assertThat(before.getString(4).toString()).isEqualTo("val2");
        assertThat(after.getString(0).toString()).isEqualTo("106");
        assertThat(after.getString(1).toString()).isEqualTo("hammer");
        assertThat(after.getString(2).toString()).isEqualTo("18oz carpenter hammer");
        assertThat(after.getString(3).toString()).isEqualTo("1.0");
        assertThat(after.getString(4).toString()).isEqualTo("val0");
    }

    @Test
    @Timeout(120)
    public void testCanalJsonFormatPreFetch() throws Exception {
        prepareData(readLines("canal-data-schema-change.txt"));

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

        Thread.sleep(10_000);
        int expectedSize = 6;
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("description", DataTypes.STRING())
                        .physicalColumn("weight", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        List<Event> actual = fetchResults(events, expectedSize);

        assertThat(actual.get(0)).isInstanceOf(CreateTableEvent.class);
        CreateTableEvent createTableEvent = (CreateTableEvent) actual.get(0);
        assertThat(createTableEvent.getSchema()).isEqualTo(expectedSchema);

        List<String> expectedDataChangeEvents =
                Arrays.asList(
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[0, test0, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[1, test1, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[2, test2, description for test2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[], after=[3, test3, description for test3, 3.0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=inventory.products, before=[3, test3, description for test3, 3.0], after=[3, test3, description for test3 update, 3.14], op=UPDATE, meta=()}");

        List<RecordData.FieldGetter> fieldGetters =
                IntStream.range(0, expectedSchema.getColumnCount())
                        .mapToObj(
                                i ->
                                        RecordData.createFieldGetter(
                                                expectedSchema.getColumnDataTypes().get(i), i))
                        .collect(Collectors.toList());

        for (int i = 1; i < expectedSize; i++) {
            assertThat(actual.get(i)).isInstanceOf(DataChangeEvent.class);
            assertThat(TestUtil.convertEventToStr(actual.get(i), fieldGetters))
                    .isEqualTo(expectedDataChangeEvents.get(i - 1));
        }
    }

    private void prepareData(List<String> values) {
        values.forEach(
                value -> {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(topic, 0, "", value);
                    producer.send(record);
                });
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    private void deleteTestTopic(String topic) throws ExecutionException, InterruptedException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        return standardProps;
    }

    private List<String> readLines(String resource) throws IOException {
        final URL url = KafkaDataSourceITCase.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private List<Event> fetchResults(Iterator<Event> iter, int size) {
        List<Event> result = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            result.add(iter.next());
            size--;
        }
        return result;
    }
}
