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

package org.apache.flink.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSink.detectVersion;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test cases for {@link StarRocksDataSink}. */
class StarRocksDataSinkTest {

    @Test
    void testDetectSinkVersion() {
        assertThat(chooseSinkVersionWithOptions(Collections.emptyMap()))
                .isEqualTo(StarRocksDataSink.SinkVersion.V1);
        assertThat(chooseSinkVersionWithOptions(ImmutableMap.of("sink.version", "v1")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V1);
        assertThat(chooseSinkVersionWithOptions(ImmutableMap.of("sink.version", "V1")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V1);
        assertThat(chooseSinkVersionWithOptions(ImmutableMap.of("sink.version", "v2")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V2);
        assertThat(chooseSinkVersionWithOptions(ImmutableMap.of("sink.version", "V2")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V2);

        assertThat(chooseSinkVersionWithOptions(ImmutableMap.of("sink.version", "auto")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V2);
        assertThat(
                        chooseSinkVersionWithOptions(
                                ImmutableMap.of(
                                        "sink.version", "auto", "sink.semantic", "exactly-once")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V1);

        assertThat(
                        chooseSinkVersionWithOptions(
                                ImmutableMap.of(
                                        "sink.version", "auto", "sink.semantic", "at-least-once")))
                .isEqualTo(StarRocksDataSink.SinkVersion.V2);
    }

    StarRocksDataSink.SinkVersion chooseSinkVersionWithOptions(Map<String, String> options) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder();
        options.forEach(builder::withProperty);
        return detectVersion(builder.build());
    }
}
