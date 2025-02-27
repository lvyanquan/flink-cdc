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

package org.apache.flink.cdc.connectors.sls.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.inference.SchemaInferenceSourceOptions;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.sls.TestUtil;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.alibaba.ververica.connectors.sls.source.StartupMode;
import com.alibaba.ververica.connectors.sls.testutil.SlsSourceTestEnv;
import com.shade.aliyun.openservices.log.common.LogItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link SlsDataSource}. */
public class SlsDataSourceITCase {
    private static final String LOGSTORE_PREFIX = "it-data-source-test-";

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private String logstoreName;

    @BeforeAll
    public static void setUp() {
        SlsSourceTestEnv.setup();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        env.setParallelism(2);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        logstoreName = LOGSTORE_PREFIX + System.currentTimeMillis();
        SlsSourceTestEnv.createLogstore(logstoreName, 1, true);
        SlsSourceTestEnv.waitForOperation();
    }

    @AfterEach
    public void afterEach() throws Exception {
        SlsSourceTestEnv.deleteLogstore(logstoreName);
    }

    @Test
    @Timeout(120)
    public void testStaticInferenceStrategy() throws Exception {
        Map<String, String> logData = new HashMap<>();
        logData.put("id", "0");
        logData.put("name", "test0");
        writeLog(logData);
        Thread.sleep(3000);

        SlsDataSourceFactory sourceFactory = new SlsDataSourceFactory();
        Map<String, String> config = new HashMap<>();
        config.put(SlsOptions.ENDPOINT.key(), SlsSourceTestEnv.endpoint);
        config.put(SlsOptions.PROJECT.key(), SlsSourceTestEnv.project);
        config.put(SlsOptions.ACCESS_ID.key(), SlsSourceTestEnv.accessId);
        config.put(SlsOptions.ACCESS_KEY.key(), SlsSourceTestEnv.accessKey);
        config.put(SlsOptions.LOGSTORE.key(), logstoreName);
        config.put(SlsOptions.STARTUP_MODE.key(), StartupMode.EARLIEST.name());

        config.put(SchemaInferenceSourceOptions.SCHEMA_INFERENCE_STRATEGY.key(), "static");
        config.put(SlsOptions.SCAN_MAX_PRE_FETCH_LOG_GROUPS.key(), "1");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SlsDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Thread.sleep(5_000);
        // produce some new data
        Map<String, String> logData1 = new HashMap<>();
        logData1.put("id", "1");
        logData1.put("name", "test1");
        logData1.put("age", "10");
        Map<String, String> logData2 = new HashMap<>();
        logData2.put("id", "2");
        logData2.put("desc", "desc for 2");
        writeLogs(Arrays.asList(logData1, logData2));

        Schema expectedSchema = Schema.newBuilder().build();
        List<Event> actual = fetchResults(events, 4);
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s.%s, schema=columns={`name` STRING,`id` STRING}, primaryKeys=, options=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[test0, 0], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[test1, 1], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[null, 2], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName));
        assertThat(actualReadableEvents).isEqualTo(expectedEvents);
    }

    @Test
    @Timeout(120)
    public void testContinuousInferenceStrategy() throws Exception {
        Map<String, String> logData0 = new HashMap<>();
        logData0.put("id", "0");
        logData0.put("name", "test0");
        Map<String, String> logData1 = new HashMap<>();
        logData1.put("id", "1");
        logData1.put("name", "test1");
        logData1.put("age", "10");
        Map<String, String> logData2 = new HashMap<>();
        logData2.put("id", "2");
        logData2.put("name", "test2");
        logData2.put("weight", "3.14");
        logData2.put("desc", "desc for 2");
        writeLogs(Arrays.asList(logData0, logData1, logData2));

        SlsDataSourceFactory sourceFactory = new SlsDataSourceFactory();
        Map<String, String> config = new HashMap<>();
        config.put(SlsOptions.ENDPOINT.key(), SlsSourceTestEnv.endpoint);
        config.put(SlsOptions.PROJECT.key(), SlsSourceTestEnv.project);
        config.put(SlsOptions.ACCESS_ID.key(), SlsSourceTestEnv.accessId);
        config.put(SlsOptions.ACCESS_KEY.key(), SlsSourceTestEnv.accessKey);
        config.put(SlsOptions.LOGSTORE.key(), logstoreName);
        config.put(SlsOptions.STARTUP_MODE.key(), StartupMode.EARLIEST.name());
        config.put(SlsOptions.SCAN_MAX_PRE_FETCH_LOG_GROUPS.key(), "0");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        sourceFactory
                                .createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(config),
                                                Configuration.fromMap(new HashMap<>()),
                                                this.getClass().getClassLoader()))
                                .getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SlsDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Schema expectedSchema = Schema.newBuilder().build();
        List<Event> actual = fetchResults(events, 6);
        List<String> actualReadableEvents = new ArrayList<>();
        for (Event event : actual) {
            if (event instanceof SchemaChangeEvent) {
                expectedSchema =
                        SchemaUtils.applySchemaChangeEvent(
                                expectedSchema, (SchemaChangeEvent) event);
            }
            actualReadableEvents.add(TestUtil.convertEventToStr(event, expectedSchema));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        String.format(
                                "CreateTableEvent{tableId=%s.%s, schema=columns={`name` STRING,`id` STRING}, primaryKeys=, options=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[test0, 0], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "AddColumnEvent{tableId=%s.%s, addedColumns=[ColumnWithPosition{column=`age` STRING, position=AFTER, existedColumnName=id}]}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[test1, 1, 10], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "AddColumnEvent{tableId=%s.%s, addedColumns=[ColumnWithPosition{column=`weight` STRING, position=AFTER, existedColumnName=age}, ColumnWithPosition{column=`desc` STRING, position=AFTER, existedColumnName=weight}]}",
                                SlsSourceTestEnv.project, logstoreName),
                        String.format(
                                "DataChangeEvent{tableId=%s.%s, before=[], after=[test2, 2, null, 3.14, desc for 2], op=INSERT, meta=()}",
                                SlsSourceTestEnv.project, logstoreName));
        assertThat(actualReadableEvents).isEqualTo(expectedEvents);
    }

    private List<Event> fetchResults(Iterator<Event> iter, int size) {
        List<Event> result = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            result.add(iter.next());
            size--;
        }
        return result;
    }

    private void writeLogs(List<Map<String, String>> logs) {
        List<LogItem> logItems =
                logs.stream()
                        .map(
                                logData -> {
                                    LogItem logItem = new LogItem();
                                    logData.forEach(logItem::PushBack);
                                    return logItem;
                                })
                        .collect(Collectors.toList());
        SlsSourceTestEnv.writeLogs(logItems, logstoreName);
    }

    private void writeLog(Map<String, String> logData) {
        LogItem logItem = new LogItem();
        logData.forEach(logItem::PushBack);
        SlsSourceTestEnv.writeLogs(Collections.singletonList(logItem), logstoreName);
    }
}
