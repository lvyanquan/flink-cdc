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

package org.apache.flink.cdc.connectors.kafka;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Test utility. */
public class TestUtil {

    public static List<String> readLines(String resource) throws IOException {
        final URL url = TestUtil.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    public static List<RecordData.FieldGetter> getFieldGettersBySchema(Schema schema) {
        return IntStream.range(0, schema.getColumnCount())
                .mapToObj(i -> RecordData.createFieldGetter(schema.getColumnDataTypes().get(i), i))
                .collect(Collectors.toList());
    }

    public static String convertEventToStr(Event event, List<RecordData.FieldGetter> fieldGetters) {
        if (event instanceof SchemaChangeEvent) {
            return event.toString();
        } else if (event instanceof DataChangeEvent) {
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
}
