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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaDataSourceFactory}. */
public class KafkaDataSourceFactoryTest {

    @Test
    public void testCreateDataSource() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(KafkaDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("properties.bootstrap.servers", "test")
                                .put("scan.startup.mode", "latest-offset")
                                .put("debezium-json.schema-include", "true")
                                .put("topic", "test-topic")
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSource).isInstanceOf(KafkaDataSource.class);
    }

    @Test
    public void testCreateDataSourceMissingRequiredOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(KafkaDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder().put("topic", "test-topic").build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "One or more required options are missing.\n\n"
                                + "Missing required options are:\n\n"
                                + "properties.bootstrap.servers");
    }

    @Test
    public void testUnsupportedOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(KafkaDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("properties.bootstrap.servers", "test")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'kafka'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testPrefixRequireOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("kafka", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(KafkaDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("properties.bootstrap.servers", "test")
                                .put("properties.compression.type", "none")
                                .build());
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(KafkaDataSource.class);
    }
}
