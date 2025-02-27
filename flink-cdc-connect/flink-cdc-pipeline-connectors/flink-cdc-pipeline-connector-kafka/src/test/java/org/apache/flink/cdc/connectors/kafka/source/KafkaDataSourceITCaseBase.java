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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/** IT tests base class for {@link KafkaDataSource}. */
public abstract class KafkaDataSourceITCaseBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaDataSourceITCaseBase.class);
    protected static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    protected static final Network NETWORK = Network.newNetwork();
    protected static final int ZK_TIMEOUT_MILLIS = 30000;
    protected static final short TOPIC_REPLICATION_FACTOR = 1;
    protected static AdminClient admin;
    protected static final String KAFKA = "reg.docker.alibaba-inc.com/confluentinc/cp-kafka:7.2.2";

    protected final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    protected String topic;
    protected KafkaProducer<String, String> producer;

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

    protected void prepareValues(List<String> values) {
        values.forEach(
                value -> {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(topic, 0, "", value);
                    producer.send(record);
                });
    }

    protected void prepareRecords(List<Tuple2<String, String>> records) {
        records.forEach(
                record -> {
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(topic, 0, record.f0, record.f1);
                    producer.send(producerRecord);
                });
    }

    protected void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        final CreateTopicsResult result =
                admin.createTopics(
                        Collections.singletonList(
                                new NewTopic(topic, numPartitions, replicationFactor)));
        result.all().get();
    }

    protected void deleteTestTopic(String topic) throws ExecutionException, InterruptedException {
        final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
        result.all().get();
    }

    protected static Properties getKafkaClientConfiguration() {
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

    protected List<String> readLines(String resource) throws IOException {
        final URL url = KafkaDataSourceITCase.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    protected List<Event> fetchResults(Iterator<Event> iter, int size) {
        List<Event> result = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            result.add(iter.next());
            size--;
        }
        return result;
    }

    protected boolean equalsWithPattern(List<String> expectedPatterns, List<String> actual) {
        if (expectedPatterns.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expectedPatterns.size(); i++) {
            Pattern pattern = Pattern.compile(expectedPatterns.get(i));
            if (!pattern.matcher(actual.get(i)).matches()) {
                return false;
            }
        }
        return true;
    }
}
