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

package org.apache.flink.cdc.connectors.kafka.aliyun;

import com.aliyun.auth.credentials.Credential;
import com.aliyun.auth.credentials.provider.StaticCredentialProvider;
import com.aliyun.sdk.service.alikafka20190916.AsyncClient;
import com.aliyun.sdk.service.alikafka20190916.models.CreateTopicRequest;
import com.aliyun.sdk.service.alikafka20190916.models.CreateTopicResponse;
import com.aliyun.sdk.service.alikafka20190916.models.GetTopicListRequest;
import com.aliyun.sdk.service.alikafka20190916.models.GetTopicListResponse;
import com.aliyun.sdk.service.alikafka20190916.models.GetTopicListResponseBody;
import darabonba.core.client.ClientOverrideConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An aliyun Kafka client which used to create and delete topics.
 *
 * <p>Open source kafka admin client cannot create/delete topics in the aliyun kafka. The kafka
 * client provided by aliyun kafka must be used when operating topics. But the open source
 * consumer/producer APIs could be used in the aliyun kafka.
 */
public class AliyunKafkaClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunKafkaClient.class);

    private final AliyunKafkaClientParams params;
    private AsyncClient kafkaClient;

    public AliyunKafkaClient(AliyunKafkaClientParams params) {
        this.params = params;
    }

    public void open() {
        this.kafkaClient = initKafkaClient();
    }

    @Override
    public void close() {
        if (kafkaClient != null) {
            kafkaClient.close();
        }
    }

    // TODO Support to provide partition number
    public void createTopic(String topic, boolean compactTopic, long timeout, TimeUnit unit)
            throws Exception {
        CreateTopicRequest createTopicRequest =
                CreateTopicRequest.builder()
                        .instanceId(params.getInstanceId())
                        .topic(topic)
                        .remark("[C] " + params.getAccessKeyId())
                        .regionId(params.getRegionId())
                        .compactTopic(compactTopic)
                        .build();
        CompletableFuture<CreateTopicResponse> future = kafkaClient.createTopic(createTopicRequest);
        CreateTopicResponse response = future.get(timeout, unit);
        LOG.info("Creating topic by aliyun client succeed.");
        response.getBody();
    }

    public List<String> listTopic(long timeout, TimeUnit unit) throws Exception {
        GetTopicListRequest getTopicListRequest =
                GetTopicListRequest.builder().instanceId(params.getInstanceId()).build();
        CompletableFuture<GetTopicListResponse> future =
                kafkaClient.getTopicList(getTopicListRequest);
        // Synchronously get the return value of the API request
        GetTopicListResponse response = future.get(timeout, unit);
        LOG.info("Getting topic list from aliyun client succeed.");
        return response.getBody().getTopicList().getTopicVO().stream()
                .map(GetTopicListResponseBody.TopicVO::getTopic)
                .collect(Collectors.toList());
    }

    private AsyncClient initKafkaClient() {
        StaticCredentialProvider provider =
                StaticCredentialProvider.create(
                        Credential.builder()
                                .accessKeyId(params.getAccessKeyId())
                                .accessKeySecret(params.getAccessKeySecret())
                                .build());

        return AsyncClient.builder()
                .region(params.getRegionId())
                .credentialsProvider(provider)
                .overrideConfiguration(
                        ClientOverrideConfiguration.create()
                                .setEndpointOverride(params.getEndpoint()))
                .build();
    }
}
