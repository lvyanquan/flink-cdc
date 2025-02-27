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

package org.apache.flink.cdc.connectors.sls.source.reader.deserializer;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.sls.TestUtil;
import org.apache.flink.util.Collector;

import com.aliyun.openservices.log.common.FastLog;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SlsLogStaticDeserializationSchema}. */
public class SlsLogStaticDeserializationSchemaTest {

    public static final TableId TABLE_ID = TableId.tableId("test_project", "test_logstore");
    public static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("f0", DataTypes.STRING())
                    .physicalColumn("f1", DataTypes.STRING())
                    .physicalColumn("f2", DataTypes.STRING())
                    .build();

    @Test
    public void testDeserializeWithoutInitialSchema() {
        SlsLogStaticDeserializationSchema deserializationSchema =
                new SlsLogStaticDeserializationSchema();
        SimpleCollector collector = new SimpleCollector();

        assertThatThrownBy(() -> deserializationSchema.deserialize(null, TABLE_ID, collector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(String.format("Unknown initial schema for table <%s>.", TABLE_ID));

        assertThat(deserializationSchema.getTableSchemas()).isEmpty();
        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();

        deserializationSchema.setTableSchema(TABLE_ID, SCHEMA);
        assertThatNoException()
                .isThrownBy(() -> deserializationSchema.deserialize(null, TABLE_ID, collector));

        TableId tableId =
                TableId.tableId(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        assertThatThrownBy(() -> deserializationSchema.deserialize(null, tableId, collector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(String.format("Unknown initial schema for table <%s>.", tableId));

        assertThat(deserializationSchema.getAlreadySendCreateTableTables()).isEmpty();
    }

    @Test
    public void testDeserialize() throws Exception {
        SlsLogStaticDeserializationSchema deserializationSchema =
                new SlsLogStaticDeserializationSchema();
        deserializationSchema.setTableSchema(TABLE_ID, SCHEMA);
        SimpleCollector collector = new SimpleCollector();

        Map<String, String> keyValues = new HashMap<>();
        keyValues.put("f0", "value0");
        keyValues.put("f1", "value1");
        FastLog fastLog = TestUtil.buildFastLog(keyValues);

        keyValues.clear();
        keyValues.put("f2", "value2");
        keyValues.put("f3", "value3");
        FastLog fastLog2 = TestUtil.buildFastLog(keyValues);

        keyValues.clear();
        keyValues.put("f0", "value0");
        keyValues.put("f1", "value1");
        keyValues.put("f2", "value2");
        keyValues.put("f3", "value3");
        FastLog fastLog3 = TestUtil.buildFastLog(keyValues);

        for (FastLog log : Arrays.asList(fastLog, fastLog2, fastLog3)) {
            deserializationSchema.deserialize(log, TABLE_ID, collector);
        }

        List<String> actualEvents = new ArrayList<>();
        for (Event event : collector.getList()) {
            actualEvents.add(TestUtil.convertEventToStr(event, SCHEMA));
        }
        List<String> expectedEvents =
                Arrays.asList(
                        "CreateTableEvent{tableId=test_project.test_logstore, schema=columns={`f0` STRING,`f1` STRING,`f2` STRING}, primaryKeys=, options=()}",
                        "DataChangeEvent{tableId=test_project.test_logstore, before=[], after=[value0, value1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_project.test_logstore, before=[], after=[null, null, value2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=test_project.test_logstore, before=[], after=[value0, value1, value2], op=INSERT, meta=()}");

        assertThat(actualEvents).isEqualTo(expectedEvents);

        assertThat(deserializationSchema.getTableSchemas()).hasSize(1);
        assertThat(deserializationSchema.getTableSchema(TABLE_ID)).isEqualTo(SCHEMA);
        assertThat(deserializationSchema.getAlreadySendCreateTableTables())
                .containsExactly(TABLE_ID);
    }

    /** A collector that stores elements in a list. */
    public static class SimpleCollector implements Collector<Event> {

        private final List<Event> list = new ArrayList<>();

        public List<Event> getList() {
            return list;
        }

        @Override
        public void collect(Event event) {
            list.add(event);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
