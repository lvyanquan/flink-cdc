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

package org.apache.flink.cdc.connectors.hologres;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.hologres.config.HologresCommonOption;
import org.apache.flink.cdc.connectors.hologres.factory.HologresDataSinkFactory;
import org.apache.flink.cdc.connectors.hologres.sink.HologresDataSink;
import org.apache.flink.cdc.connectors.hologres.sink.HologresMetadataApplier;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests for {@link HologresDataSinkFactory}. */
public class HologresDataSinkFactoryTest {

    @Test
    public void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hologres", DataSinkFactory.class);
        Assertions.assertInstanceOf(HologresDataSinkFactory.class, sinkFactory);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(HologresCommonOption.ENDPOINT.key(), "localhost:8080")
                                .put(HologresCommonOption.DATABASE.key(), "database")
                                .put(HologresCommonOption.USERNAME.key(), "root")
                                .put(HologresCommonOption.PASSWORD.key(), "password")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertInstanceOf(HologresDataSink.class, dataSink);

        MetadataApplier metadataApplier = dataSink.getMetadataApplier();
        Assertions.assertInstanceOf(HologresMetadataApplier.class, metadataApplier);
    }

    @Test
    void testLackRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hologres", DataSinkFactory.class);
        Assertions.assertInstanceOf(HologresDataSinkFactory.class, sinkFactory);

        Map<String, String> options =
                ImmutableMap.<String, String>builder()
                        .put(HologresCommonOption.ENDPOINT.key(), "localhost:8080")
                        .put(HologresCommonOption.DATABASE.key(), "database")
                        .put(HologresCommonOption.USERNAME.key(), "root")
                        .put(HologresCommonOption.PASSWORD.key(), "password")
                        .build();

        List<String> requireKeys =
                sinkFactory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Configuration conf = Configuration.fromMap(remainingOptions);

            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () ->
                                    sinkFactory.createDataSink(
                                            new FactoryHelper.DefaultContext(
                                                    conf,
                                                    conf,
                                                    Thread.currentThread()
                                                            .getContextClassLoader())))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    void testUnsupportedOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hologres", DataSinkFactory.class);
        Assertions.assertInstanceOf(HologresDataSinkFactory.class, sinkFactory);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(HologresCommonOption.ENDPOINT.key(), "localhost:8080")
                                .put(HologresCommonOption.DATABASE.key(), "database")
                                .put(HologresCommonOption.USERNAME.key(), "root")
                                .put(HologresCommonOption.PASSWORD.key(), "password")
                                .put(
                                        HologresCommonOption.OPTIONAL_CONNECTION_SSL_MODE.key(),
                                        "require")
                                .put(HologresCommonOption.OPTIONAL_JDBC_RETRY_COUNT.key(), "1")
                                .put(
                                        HologresCommonOption.OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS
                                                .key(),
                                        "1000")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'hologres'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hologres", DataSinkFactory.class);
        Assertions.assertInstanceOf(HologresDataSinkFactory.class, sinkFactory);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(HologresCommonOption.ENDPOINT.key(), "localhost:8080")
                                .put(HologresCommonOption.DATABASE.key(), "database")
                                .put(HologresCommonOption.USERNAME.key(), "root")
                                .put(HologresCommonOption.PASSWORD.key(), "password")
                                .put("table_property.distribution_key", "b")
                                .put("table_property.orientation", "row")
                                .put("table_property.CLUSTERING_KEY", "a")
                                .build());

        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertInstanceOf(HologresDataSink.class, dataSink);
    }

    @Test
    public void testCreateDataSinkWithNormalizeOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("hologres", DataSinkFactory.class);
        Assertions.assertInstanceOf(HologresDataSinkFactory.class, sinkFactory);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(HologresCommonOption.ENDPOINT.key(), "localhost:8080")
                                .put(HologresCommonOption.DATABASE.key(), "database")
                                .put(HologresCommonOption.USERNAME.key(), "root")
                                .put(HologresCommonOption.PASSWORD.key(), "password")
                                .put("jdbcEnableDefaultForNotNullColumn", "false")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertInstanceOf(HologresDataSink.class, dataSink);

        MetadataApplier metadataApplier = dataSink.getMetadataApplier();
        Assertions.assertInstanceOf(HologresMetadataApplier.class, metadataApplier);
    }
}
