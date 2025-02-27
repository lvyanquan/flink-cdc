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

package org.apache.flink.cdc.connectors.sls.source.enumerator;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.configuration.Configuration;

import com.alibaba.ververica.connectors.sls.SLSOptions;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.assigner.ShardAssigner;
import com.alibaba.ververica.connectors.sls.newsource.enumerator.initializer.CursorInitializer;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStore;
import com.alibaba.ververica.connectors.sls.newsource.model.LogStoreShard;
import com.alibaba.ververica.connectors.sls.newsource.split.SlsShardSplit;
import com.alibaba.ververica.connectors.sls.testutil.SlsSourceTestEnv;
import com.shade.aliyun.openservices.log.common.LogItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PipelineSlsSourceEnumerator}. */
public class PipelineSlsSourceEnumeratorTest {
    private static final String LOGSTORE_PREFIX = "ut-pipeline-enumerator-test-";

    private String logstoreName;

    @BeforeAll
    public static void setUp() {
        SlsSourceTestEnv.setup();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        logstoreName = LOGSTORE_PREFIX + System.currentTimeMillis();
        SlsSourceTestEnv.createLogstore(logstoreName, 1, true);
        SlsSourceTestEnv.waitForOperation();
    }

    @AfterEach
    public void afterEach() throws Exception {
        SlsSourceTestEnv.deleteLogstore(logstoreName);
    }

    @Test
    public void testGetSchemaByParsingRecord() throws Exception {
        try (MockSplitEnumeratorContext<SlsShardSplit> context =
                        new MockSplitEnumeratorContext<>(1);
                PipelineSlsSourceEnumerator enumerator =
                        createEnumerator(logstoreName, 2, context)) {

            enumerator.start();
            CursorInitializer.ShardCursorRetriever retriever = enumerator.getCursorRetriever();

            LogStore logStore = new LogStore(SlsSourceTestEnv.project, logstoreName);
            LogStoreShard shard = new LogStoreShard(0, false, "", "");
            // Set cursor to latest timestamp
            String cursor = retriever.cursorForTime(shard, Integer.MAX_VALUE);
            SlsShardSplit split = new SlsShardSplit(logStore, shard, cursor, null);

            // Get empty schema when there is no log
            Schema schema = enumerator.getSchemaByParsingRecord(split);
            assertThat(schema.getColumnCount()).isEqualTo(0);

            Schema expectedSchema =
                    Schema.newBuilder()
                            .physicalColumn("f0", DataTypes.STRING())
                            .physicalColumn("f1", DataTypes.STRING())
                            .physicalColumn("f2", DataTypes.STRING())
                            .physicalColumn("f3", DataTypes.STRING())
                            .build();

            LogItem logItem = new LogItem();
            expectedSchema.getColumnNames().forEach(key -> logItem.PushBack(key, ""));
            SlsSourceTestEnv.writeLogs(Collections.singletonList(logItem), logstoreName);

            assertThat(enumerator.getSchemaByParsingRecord(split)).isEqualTo(expectedSchema);
        }
    }

    private PipelineSlsSourceEnumerator createEnumerator(
            String logstoreName,
            int maxFetchLogGroups,
            MockSplitEnumeratorContext<SlsShardSplit> enumContext) {
        Configuration config = SlsSourceTestEnv.getConfiguration();
        config.set(SLSOptions.LOGSTORE, logstoreName);

        return new PipelineSlsSourceEnumerator(
                config,
                CursorInitializer.latest(),
                CursorInitializer.noStopping(),
                ShardAssigner.roundRobin(),
                enumContext,
                maxFetchLogGroups);
    }
}
