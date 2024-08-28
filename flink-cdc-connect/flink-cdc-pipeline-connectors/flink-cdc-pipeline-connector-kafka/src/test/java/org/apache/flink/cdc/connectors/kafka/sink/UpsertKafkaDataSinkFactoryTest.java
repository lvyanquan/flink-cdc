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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_AK;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_ENDPOINT;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_INSTANCE_ID;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_REGION_ID;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_SK;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UpsertKafkaDataSinkFactory}. */
public class UpsertKafkaDataSinkFactoryTest {

    @Test
    public void testCreateUpsertDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("upsert-kafka", DataSinkFactory.class);
        Assertions.assertTrue(sinkFactory instanceof UpsertKafkaDataSinkFactory);

        Configuration conf = Configuration.fromMap(new HashMap<>());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertTrue(dataSink instanceof UpsertKafkaDataSink);
    }

    @Test
    public void testCreateUpsertDataSinkInAliyun() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("upsert-kafka", DataSinkFactory.class);
        Assertions.assertTrue(sinkFactory instanceof UpsertKafkaDataSinkFactory);

        Configuration pipelineConfiguration = Configuration.fromMap(new HashMap<>());
        Map<String, String> factoryOptions = new HashMap<>();
        factoryOptions.put(ALIYUN_KAFKA_AK.key(), "1");
        factoryOptions.put(ALIYUN_KAFKA_SK.key(), "2");
        factoryOptions.put(ALIYUN_KAFKA_REGION_ID.key(), "3");
        factoryOptions.put(ALIYUN_KAFKA_ENDPOINT.key(), "4");
        factoryOptions.put(ALIYUN_KAFKA_INSTANCE_ID.key(), "5");
        factoryOptions.put("properties.bootstrap.servers", "test1");
        factoryOptions.put("properties.test", "test2");
        Configuration factoryConfiguration = Configuration.fromMap(factoryOptions);
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                factoryConfiguration,
                                pipelineConfiguration,
                                Thread.currentThread().getContextClassLoader()));
        Assertions.assertTrue(dataSink instanceof UpsertKafkaDataSink);

        UpsertKafkaMetadataApplier applier = (UpsertKafkaMetadataApplier) dataSink.getMetadataApplier();
        assertThat(applier.getBootstrapServers()).isEqualTo("test1");
        assertThat(applier.getKafkaProperties().getProperty("test")).isEqualTo("test2");
        assertThat(applier.getAliyunKafkaParams().getAccessKeyId()).isEqualTo("1");
        assertThat(applier.getAliyunKafkaParams().getAccessKeySecret()).isEqualTo("2");
        assertThat(applier.getAliyunKafkaParams().getRegionId()).isEqualTo("3");
        assertThat(applier.getAliyunKafkaParams().getEndpoint()).isEqualTo("4");
        assertThat(applier.getAliyunKafkaParams().getInstanceId()).isEqualTo("5");
    }
}
