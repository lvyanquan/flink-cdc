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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafka.aliyun.AliyunKafkaClient;
import org.apache.flink.cdc.connectors.kafka.aliyun.AliyunKafkaClientParams;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A {@code MetadataApplier} that applies metadata changes to Upsert Kafka. Support primary key
 * table only.
 */
public class KafkaMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataApplier.class);

    private static final Duration ADMIN_CLIENT_TIMEOUT = Duration.ofMinutes(1);

    private final boolean isUpsertKafka;
    private final String bootstrapServers;
    private final Properties kafkaProperties;
    @Nullable private final AliyunKafkaClientParams aliyunKafkaParams;

    @Nullable private AdminClient adminClient;
    @Nullable private AliyunKafkaClient aliyunKafkaClient;

    public KafkaMetadataApplier(
            boolean isUpsertKafka,
            Properties kafkaProperties,
            AliyunKafkaClientParams aliyunKafkaParams) {
        this.isUpsertKafka = isUpsertKafka;
        this.bootstrapServers = kafkaProperties.getProperty("bootstrap.servers");
        this.kafkaProperties = kafkaProperties;
        this.aliyunKafkaParams = aliyunKafkaParams;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        try {
            if (schemaChangeEvent instanceof CreateTableEvent) {

                int tries = 0;
                int createRetryTimes = 5;
                String topicName = schemaChangeEvent.tableId().getTableName();

                if (aliyunKafkaParams != null) {
                    if (aliyunKafkaClient == null) {
                        this.aliyunKafkaClient = new AliyunKafkaClient(aliyunKafkaParams);
                        aliyunKafkaClient.open();
                    }

                    while (tries++ < createRetryTimes) {
                        try {
                            aliyunKafkaClient.createTopic(
                                    topicName,
                                    isUpsertKafka,
                                    ADMIN_CLIENT_TIMEOUT.getSeconds(),
                                    TimeUnit.SECONDS);
                            LOG.info(
                                    "Create compacted topic {} succeed in aliyun kafka.",
                                    topicName);
                            return;
                        } catch (Throwable t) {
                            if (tries < createRetryTimes) {
                                LOG.warn(
                                        String.format(
                                                "Fail to create topic %s at the %d time.",
                                                topicName, tries),
                                        t);
                                try {
                                    // sleep 1 s because aliyun kafka limits 1 QPS
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                }
                                continue;
                            }
                            throw new CatalogException(
                                    String.format(
                                            "Fail to create topic [%s] in aliyun kafka.",
                                            topicName),
                                    t);
                        }
                    }
                } else {
                    if (adminClient == null) {
                        adminClient = getKafkaAdminClient();
                    }

                    if (isUpsertKafka) {
                        while (tries++ < createRetryTimes) {
                            try {
                                Map<String, String> newTopicConfig = new HashMap<>();
                                newTopicConfig.put(
                                        TopicConfig.CLEANUP_POLICY_CONFIG,
                                        TopicConfig.CLEANUP_POLICY_COMPACT);
                                NewTopic newTopic =
                                        new NewTopic(topicName, Optional.empty(), Optional.empty());
                                newTopic = newTopic.configs(newTopicConfig);
                                CreateTopicsResult result =
                                        adminClient.createTopics(Collections.singleton(newTopic));
                                KafkaFuture<Void> future = result.values().get(topicName);
                                future.get(ADMIN_CLIENT_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                                LOG.info(
                                        "Create compacted topic {} succeed through admin client.",
                                        topicName);
                                return;
                            } catch (Throwable t) {
                                if (tries < createRetryTimes) {
                                    LOG.warn(
                                            String.format(
                                                    "Fail to create topic %s at the %d time.",
                                                    topicName, tries),
                                            t);
                                    try {
                                        Thread.sleep(500);
                                    } catch (InterruptedException e) {
                                    }
                                    continue;
                                }
                                throw new CatalogException(
                                        String.format("Fail to create topic [%s].", topicName), t);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private AdminClient getKafkaAdminClient() {
        final Properties adminClientProps = new Properties();
        deepCopyProperties(kafkaProperties, adminClientProps);
        // set client id prefix
        adminClientProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClientProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG,
                "cdc-pipeline-" + ThreadLocalRandom.current().nextLong());
        return AdminClient.create(adminClientProps);
    }

    private void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    @VisibleForTesting
    public boolean isUpsertKafka() {
        return isUpsertKafka;
    }

    @VisibleForTesting
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @VisibleForTesting
    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    @VisibleForTesting
    @Nullable
    public AliyunKafkaClientParams getAliyunKafkaParams() {
        return aliyunKafkaParams;
    }
}
