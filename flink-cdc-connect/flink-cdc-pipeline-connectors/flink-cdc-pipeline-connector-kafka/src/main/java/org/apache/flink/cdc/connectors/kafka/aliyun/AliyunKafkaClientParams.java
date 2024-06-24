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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_AK;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_ENDPOINT;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_INSTANCE_ID;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_REGION_ID;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.ALIYUN_KAFKA_SK;

/** The parameters used to create an {@link AliyunKafkaClient}. */
public class AliyunKafkaClientParams implements Serializable {
    private static final List<ConfigOption<String>> REQUIRED_OPTIONS =
            Arrays.asList(
                    ALIYUN_KAFKA_AK,
                    ALIYUN_KAFKA_SK,
                    ALIYUN_KAFKA_ENDPOINT,
                    ALIYUN_KAFKA_REGION_ID,
                    ALIYUN_KAFKA_INSTANCE_ID);
    private static final String ERROR_MSG = "%s option is required, but it is not configured yet.";

    private final String accessKeyId;
    private final String accessKeySecret;
    private final String endpoint;
    private final String regionId;
    private final String instanceId;

    public AliyunKafkaClientParams(
            String accessKeyId,
            String accessKeySecret,
            String endpoint,
            String regionId,
            String instanceId) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.endpoint = endpoint;
        this.regionId = regionId;
        this.instanceId = instanceId;
    }

    @Nullable
    public static AliyunKafkaClientParams createAliyunKafkaClientParams(
            Configuration catalogOptions) {
        String instanceId = catalogOptions.getOptional(ALIYUN_KAFKA_INSTANCE_ID).orElse(null);
        if (instanceId == null) {
            for (ConfigOption<String> option : REQUIRED_OPTIONS) {
                if (catalogOptions.getOptional(option).orElse(null) != null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "%s option is not supported for current Kafka Cluster, it only works for Aliyun Kafka Cluster.",
                                    option.key()));
                }
            }
            return null;
        } else {
            return new AliyunKafkaClientParams(
                    checkNotNull(
                            catalogOptions.getOptional(ALIYUN_KAFKA_AK).orElse(null),
                            String.format(ERROR_MSG, ALIYUN_KAFKA_AK.key())),
                    checkNotNull(
                            catalogOptions.getOptional(ALIYUN_KAFKA_SK).orElse(null),
                            String.format(ERROR_MSG, ALIYUN_KAFKA_SK.key())),
                    checkNotNull(
                            catalogOptions.getOptional(ALIYUN_KAFKA_ENDPOINT).orElse(null),
                            String.format(ERROR_MSG, ALIYUN_KAFKA_ENDPOINT.key())),
                    checkNotNull(
                            catalogOptions.getOptional(ALIYUN_KAFKA_REGION_ID).orElse(null),
                            String.format(ERROR_MSG, ALIYUN_KAFKA_REGION_ID.key())),
                    instanceId);
        }
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AliyunKafkaClientParams)) {
            return false;
        }
        AliyunKafkaClientParams that = (AliyunKafkaClientParams) o;
        return accessKeyId.equals(that.accessKeyId)
                && accessKeySecret.equals(that.accessKeySecret)
                && endpoint.equals(that.endpoint)
                && regionId.equals(that.regionId)
                && instanceId.equals(that.instanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKeyId, accessKeySecret, endpoint, regionId, instanceId);
    }
}
