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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkDataStreamSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for using {@link KafkaDataSink} writing to a Kafka cluster. */
class UpsertKafkaDataSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(UpsertKafkaDataSinkITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static AdminClient admin;
    private static final String KAFKA = "reg.docker.alibaba-inc.com/confluentinc/cp-kafka:7.2.2";

    private String topic;

    private TableId table1;

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
    public static void teardownAdmin() {
        admin.close();
        KAFKA_CONTAINER.stop();
    }

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        table1 =
                TableId.tableId(
                        "default_namespace", "default_schema", UUID.randomUUID().toString());
        topic = table1.toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        deleteTestTopic(topic);
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        final CreateTopicsResult result =
                admin.createTopics(
                        getCreateTopicRequest(topic, numPartitions, replicationFactor, true));
        result.all().get();
    }

    private Collection<NewTopic> getCreateTopicRequest(
            String topicName, int numPartitions, short replicationFactor, boolean compactTopic) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        if (compactTopic) {
            Map<String, String> newTopicConfig = new HashMap<>();
            newTopicConfig.put(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            newTopic = newTopic.configs(newTopicConfig);
        }
        return Collections.singletonList(newTopic);
    }

    private void deleteTestTopic(String topic) throws ExecutionException, InterruptedException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    private List<Event> createSourceEvents() {
        List<Event> events = new ArrayList<>();
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .primaryKey("col2")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        events.add(createTableEvent);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()));
        // insert: col1, col2, col3, record size: 1
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("3")
                                }));
        events.add(insertEvent1);
        // insert: col1, col2, col3, record size: 2
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4")
                                }));
        events.add(insertEvent2);
        // insert: col1, col2, col3, record size: 3
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("5")
                                }));
        events.add(insertEvent3);

        // add column: col1, col2, col3, col4, record size: 3
        AddColumnEvent.ColumnWithPosition columnWithPosition =
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("col4", DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(table1, Collections.singletonList(columnWithPosition));
        events.add(addColumnEvent);

        // drop column: col1, col2, col4, record size: 3
        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(table1, Collections.singletonList("col3"));
        events.add(dropColumnEvent);

        // not support rename event

        // delete: col1, col2, col4, record size: 2
        events.add(
                DataChangeEvent.deleteEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2"),
                                    null
                                })));

        // update: col1, col2, newCol4, record size: 2
        events.add(
                DataChangeEvent.updateEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4"),
                                    null
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("50")
                                })));
        return events;
    }

    @Test
    void testJsonFormat() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        Tuple2<DataStream<Event>, Sink<Event>> inputAndSink =
                ((FlinkDataStreamSinkProvider)
                                (new UpsertKafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .consumeDataStream(source);
        inputAndSink.f0.sinkTo(inputAndSink.f1);
        env.execute();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final String createTable =
                String.format(
                        "CREATE TABLE upsert_kafka (\n"
                                + "  `k_col2` STRING,\n"
                                + "  `col1` STRING,\n"
                                + "  `col2` STRING,\n"
                                + "  `col4` STRING,\n"
                                + "  PRIMARY KEY (k_col2) NOT ENFORCED"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = 'json',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  'sink.buffer-flush.max-rows' = '2',\n"
                                + "  'sink.buffer-flush.interval' = '100000',\n"
                                + "  'value.format' = 'json',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic, KAFKA_CONTAINER.getBootstrapServers());
        tEnv.executeSql(createTable);

        final List<Row> expected =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "2", "1", "2", null),
                        Row.ofKind(RowKind.INSERT, "3", "2", "3", null),
                        Row.ofKind(RowKind.INSERT, "4", "3", "4", null),
                        Row.ofKind(RowKind.DELETE, "2", "1", "2", null),
                        Row.ofKind(RowKind.DELETE, "4", "3", "4", null),
                        Row.ofKind(RowKind.INSERT, "4", "3", "4", "50"));
        final List<Row> result =
                collectRows(tEnv.sqlQuery("SELECT * FROM upsert_kafka"), expected.size());
        assertThat(result).containsExactlyElementsOf(expected);
        checkProducerLeak();
    }

    @Test
    void testTopicAndHeaderOption() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        env.setRestartStrategy(RestartStrategies.noRestart());
        final DataStream<Event> source =
                env.fromCollection(createSourceEvents(), new EventTypeInfo());
        Map<String, String> config = new HashMap<>();
        config.put(KafkaDataSinkOptions.TOPIC.key(), "test_topic");
        config.put(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED.key(), "true");
        Properties properties = getKafkaClientConfiguration();
        properties.forEach(
                (key, value) ->
                        config.put(
                                KafkaDataSinkOptions.PROPERTIES_PREFIX + key.toString(),
                                value.toString()));
        Tuple2<DataStream<Event>, Sink<Event>> inputAndSink =
                ((FlinkDataStreamSinkProvider)
                                (new UpsertKafkaDataSinkFactory()
                                        .createDataSink(
                                                new FactoryHelper.DefaultContext(
                                                        Configuration.fromMap(config),
                                                        Configuration.fromMap(new HashMap<>()),
                                                        this.getClass().getClassLoader()))
                                        .getEventSinkProvider()))
                        .consumeDataStream(source);
        inputAndSink.f0.sinkTo(inputAndSink.f1);
        env.execute();

        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<DeserializedRecord> expected =
                Arrays.asList(
                        new DeserializedRecord(
                                mapper.readTree("{\"col2\":\"2\"}"),
                                mapper.readTree("{\"col1\":\"1\",\"col2\":\"2\",\"col3\":\"3\"}")),
                        new DeserializedRecord(
                                mapper.readTree("{\"col2\":\"3\"}"),
                                mapper.readTree("{\"col1\":\"2\",\"col2\":\"3\",\"col3\":\"4\"}")),
                        new DeserializedRecord(
                                mapper.readTree("{\"col2\":\"4\"}"),
                                mapper.readTree("{\"col1\":\"3\",\"col2\":\"4\",\"col3\":\"5\"}")),
                        new DeserializedRecord(mapper.readTree("{\"col2\":\"2\"}"), null),
                        new DeserializedRecord(mapper.readTree("{\"col2\":\"4\"}"), null),
                        new DeserializedRecord(
                                mapper.readTree("{\"col2\":\"4\"}"),
                                mapper.readTree(
                                        "{\"col1\":\"3\",\"col2\":\"4\",\"col4\":\"50\"}")));
        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic("test_topic", false);
        assertThat(collectedRecords.size()).isEqualTo(expected.size());
        for (ConsumerRecord<byte[], byte[]> consumerRecord : collectedRecords) {
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .NAMESPACE_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getNamespace());
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .SCHEMA_NAME_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getSchemaName());
            assertThat(
                            new String(
                                    consumerRecord
                                            .headers()
                                            .headers(
                                                    PipelineKafkaRecordSerializationSchema
                                                            .TABLE_NAME_HEADER_KEY)
                                            .iterator()
                                            .next()
                                            .value()))
                    .isEqualTo(table1.getTableName());
        }
        assertThat(deserializeValues(collectedRecords)).containsAll(expected);
        checkProducerLeak();
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, boolean committed) {
        Properties properties = getKafkaClientConfiguration();
        return KafkaUtil.drainAllRecordsFromTopic(topic, properties, committed);
    }

    private List<Row> collectRows(Table table, int expectedSize) throws Exception {
        final TableResult result = table.execute();
        final List<Row> collectedRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (collectedRows.size() < expectedSize && iterator.hasNext()) {
                collectedRows.add(iterator.next());
            }
        }
        result.getJobClient()
                .ifPresent(
                        jc -> {
                            try {
                                jc.cancel().get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        return collectedRows;
    }

    private void checkProducerLeak() throws InterruptedException {
        List<Map.Entry<Thread, StackTraceElement[]>> leaks = null;
        for (int tries = 0; tries < 10; tries++) {
            leaks =
                    Thread.getAllStackTraces().entrySet().stream()
                            .filter(this::findAliveKafkaThread)
                            .collect(Collectors.toList());
            if (leaks.isEmpty()) {
                return;
            }
            Thread.sleep(1000);
        }

        for (Map.Entry<Thread, StackTraceElement[]> leak : leaks) {
            leak.getKey().stop();
        }
        fail(
                "Detected producer leaks:\n"
                        + leaks.stream().map(this::format).collect(Collectors.joining("\n\n")));
    }

    private static List<DeserializedRecord> deserializeValues(
            List<ConsumerRecord<byte[], byte[]>> records) throws IOException {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        List<DeserializedRecord> result = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            JsonNode value = null;
            if (record.value() != null) {
                value = mapper.readTree(record.value());
            }
            result.add(new DeserializedRecord(mapper.readTree(record.key()), value));
        }
        return result;
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

    private String format(Map.Entry<Thread, StackTraceElement[]> leak) {
        String stackTrace =
                Arrays.stream(leak.getValue())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n"));
        return leak.getKey().getName() + ":\n" + stackTrace;
    }

    private boolean findAliveKafkaThread(Map.Entry<Thread, StackTraceElement[]> threadStackTrace) {
        return threadStackTrace.getKey().getState() != Thread.State.TERMINATED
                && threadStackTrace.getKey().getName().contains("kafka-producer-network-thread");
    }

    static class DeserializedRecord {
        private final JsonNode key;
        private final JsonNode value;

        public DeserializedRecord(JsonNode key, JsonNode value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DeserializedRecord)) {
                return false;
            }
            DeserializedRecord that = (DeserializedRecord) o;
            return Objects.equals(key, that.key) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }
}
