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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.kafka.KafkaUtil;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/** End-to-end tests for kafka pipeline job. */
public class KafkaE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaE2eITCase.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final int PORT = 9092;
    protected static final int ZK_TIMEOUT_MILLIS = 30000;
    protected static final short TOPIC_REPLICATION_FACTOR = 1;
    private static final String KAFKA = "confluentinc/cp-kafka:7.2.2";

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            KafkaUtil.createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    private String topic;
    private AdminClient admin;
    private KafkaProducer<String, String> producer;

    @Before
    public void before() throws Exception {
        super.before();
        topic = "test-topic-" + UUID.randomUUID();

        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        admin = AdminClient.create(properties);
        producer =
                new KafkaProducer<>(
                        getKafkaClientConfiguration(),
                        new StringSerializer(),
                        new StringSerializer());

        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @After
    public void after() {
        super.after();
        admin.close();
    }

    @Test
    public void testSyncKafkaTopicJsonData() throws Exception {
        List<Tuple2<String, String>> jsonData =
                Arrays.asList(
                        Tuple2.of("{\"id\":1}", "{\"name\":\"product1\", \"weight\":1}"),
                        Tuple2.of("{\"id\":2}", "{\"name\":\"product2\", \"weight\":2}"),
                        Tuple2.of("{\"id\":3}", "{\"name\":\"product3\", \"weight\":3}"));
        prepareRecords(jsonData);
        Thread.sleep(1000);

        String pipelineJob =
                String.format(
                        "source:\n"
                                + "    type: kafka\n"
                                + "    properties.bootstrap.servers: '%s'\n"
                                + "    topic: %s\n"
                                + "    key.format: json\n"
                                + "    value.format: json\n"
                                + "    scan.startup.mode: earliest-offset\n"
                                + "    metadata.list: partition,offset,timestamp\n"
                                + "\n"
                                + "sink:\n"
                                + "    type: values\n"
                                + "\n"
                                + "transform:\n"
                                + "    - source-table: \\.*.\\.*\n"
                                + "      projection: '\\*, `partition`, `offset`, `timestamp`'\n"
                                + "      primary-keys: id\n"
                                + "\n"
                                + "pipeline:\n"
                                + "    parallelism: %s\n"
                                + "    schema.change.behavior: lenient",
                        INTER_CONTAINER_KAFKA_ALIAS + ":" + PORT, topic, parallelism);

        Path kafkaCdcJar = TestUtils.getResource("kafka-cdc-pipeline-connector.jar");
        Path valuesCdcJar = TestUtils.getResource("values-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, kafkaCdcJar, valuesCdcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        waitUntilSpecificEvent(
                String.format(
                        "CreateTableEvent{tableId=%s, schema=columns={`id` BIGINT NOT NULL,`name` STRING,`weight` BIGINT,`partition` INT NOT NULL,`offset` BIGINT NOT NULL,`timestamp` BIGINT NOT NULL}, primaryKeys=id, options=()}",
                        topic),
                60000L);

        validateEventsWithPattern(
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[1, product1, 1, 0, 0, \\d+\\], op=INSERT, meta=\\(\\{offset=0, partition=0, timestamp=\\d+\\}\\)\\}",
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[2, product2, 2, 0, 1, \\d+\\], op=INSERT, meta=\\(\\{offset=1, partition=0, timestamp=\\d+\\}\\)\\}",
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[3, product3, 3, 0, 2, \\d+\\], op=INSERT, meta=\\(\\{offset=2, partition=0, timestamp=\\d+\\}\\)\\}");

        jsonData =
                Arrays.asList(
                        Tuple2.of("{\"id\":4}", "{\"name\":\"product4\", \"weight\":4.4}"),
                        Tuple2.of("{\"id\":5}", "{\"name\":\"product5\", \"weight\":5.5}"),
                        Tuple2.of(
                                "{\"id\":6}",
                                "{\"name\":\"product6\", \"price\":60, \"weight\":6.6, \"description\":\"Description for product6\"}"));
        prepareRecords(jsonData);

        waitUntilSpecificEvent(
                String.format(
                        "AlterColumnTypeEvent{tableId=%s, typeMapping={weight=DOUBLE}, oldTypeMapping={weight=BIGINT}}",
                        topic),
                20000L);

        validateEventsWithPattern(
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[4, product4, 4.4, 0, 3, \\d+\\], op=INSERT, meta=\\(\\{offset=3, partition=0, timestamp=\\d+\\}\\)\\}",
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[5, product5, 5.5, 0, 4, \\d+\\], op=INSERT, meta=\\(\\{offset=4, partition=0, timestamp=\\d+\\}\\)\\}");

        waitUntilSpecificEvent(
                String.format(
                        "AddColumnEvent{tableId=%s, addedColumns=[ColumnWithPosition{column=`price` BIGINT, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`description` STRING, position=LAST, existedColumnName=null}]}",
                        topic),
                20000L);

        validateEventsWithPattern(
                "DataChangeEvent\\{tableId=%s, before=\\[\\], after=\\[6, product6, 6.6, 0, 5, \\d+, 60, Description for product6\\], op=INSERT, meta=\\(\\{offset=5, partition=0, timestamp=\\d+\\}\\)\\}");
    }

    private void validateEventsWithPattern(String... expectedEvents) throws Exception {
        for (String event : expectedEvents) {
            waitUntilSpecificEventWithPattern(String.format(event, topic), 20000L);
        }
    }

    private void waitUntilSpecificEvent(String event, long timeout) throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            if (stdout.contains(event)) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get specific event: "
                            + event
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    private void waitUntilSpecificEventWithPattern(String patternStr, long timeout)
            throws Exception {
        boolean result = false;
        long endTimeout = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < endTimeout) {
            String stdout = taskManagerConsumer.toUtf8String();
            Pattern pattern = Pattern.compile(patternStr);
            if (pattern.matcher(stdout).find()) {
                result = true;
                break;
            }
            Thread.sleep(1000);
        }
        if (!result) {
            throw new TimeoutException(
                    "failed to get events with pattern: "
                            + patternStr
                            + " from stdout: "
                            + taskManagerConsumer.toUtf8String());
        }
    }

    private void prepareRecords(List<Tuple2<String, String>> records) {
        records.forEach(
                record -> {
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(topic, 0, record.f0, record.f1);
                    producer.send(producerRecord);
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

    private Properties getKafkaClientConfiguration() {
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
}
