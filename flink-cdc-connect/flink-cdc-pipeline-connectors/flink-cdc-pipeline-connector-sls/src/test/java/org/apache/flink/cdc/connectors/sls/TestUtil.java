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

package org.apache.flink.cdc.connectors.sls;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.Logs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Test utility. */
public class TestUtil {

    public static String convertEventToStr(Event event, Schema schema) {
        if (event instanceof SchemaChangeEvent) {
            return event.toString();
        } else if (event instanceof DataChangeEvent) {
            List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            return "DataChangeEvent{"
                    + "tableId="
                    + dataChangeEvent.tableId()
                    + ", before="
                    + getFields(fieldGetters, dataChangeEvent.before())
                    + ", after="
                    + getFields(fieldGetters, dataChangeEvent.after())
                    + ", op="
                    + dataChangeEvent.op()
                    + ", meta="
                    + dataChangeEvent.describeMeta()
                    + '}';
        }
        throw new IllegalArgumentException("Unsupported event");
    }

    private static List<Object> getFields(
            List<RecordData.FieldGetter> fieldGetters, RecordData recordData) {
        List<Object> fields = new ArrayList<>(fieldGetters.size());
        if (recordData == null) {
            return fields;
        }
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            fields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return fields;
    }

    public static FastLog buildFastLog(Map<String, String> keyValues) {
        Logs.Log.Builder builder = Logs.Log.newBuilder();
        int timestamp = (int) (Instant.now().toEpochMilli() / 1000);
        builder.setTime(timestamp);
        keyValues.forEach((k, v) -> builder.addContents(createField(k, v)));
        return buildFastLog(builder);
    }

    public static Logs.Log.Content createField(String k, String v) {
        return Logs.Log.Content.newBuilder().setKey(k).setValue(v).build();
    }

    public static FastLog buildFastLog(Logs.Log.Builder builder) {
        Logs.Log log = builder.build();
        byte[] rawData = log.toByteArray();
        return new FastLog(rawData, 0, rawData.length);
    }
}
