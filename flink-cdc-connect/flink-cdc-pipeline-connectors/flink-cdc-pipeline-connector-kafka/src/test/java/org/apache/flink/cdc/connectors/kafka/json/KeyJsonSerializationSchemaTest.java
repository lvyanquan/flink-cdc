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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;

import static org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode.LITERAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyJsonSerializationSchema}. */
public class KeyJsonSerializationSchemaTest {

    public static final TableId TABLE_1 =
            TableId.tableId("default_namespace", "default_schema", "table1");

    @Test
    public void testSerialize() throws Exception {
        ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, false);
        SerializationSchema<Event> serializationSchema =
                new KeyJsonSerializationSchema(
                        TimestampFormat.SQL, LITERAL, "test", true, true, ZoneId.systemDefault());

        serializationSchema.open(new MockInitializationContext());
        // create table
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .physicalColumn("col3", DataTypes.STRING())
                        .primaryKey("col2")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_1, schema);
        assertThat(serializationSchema.serialize(createTableEvent)).isNull();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()));
        // insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("3")
                                }));
        assertThat(mapper.readTree(serializationSchema.serialize(insertEvent1)))
                .isEqualTo(mapper.readTree("{\"col2\":\"2\"}"));

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4")
                                }));
        assertThat(mapper.readTree(serializationSchema.serialize(insertEvent2)))
                .isEqualTo(mapper.readTree("{\"col2\":\"3\"}"));

        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3"),
                                    BinaryStringData.fromString("4"),
                                    BinaryStringData.fromString("5")
                                }));
        assertThat(mapper.readTree(serializationSchema.serialize(deleteEvent)))
                .isEqualTo(mapper.readTree("{\"col2\":\"4\"}"));

        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("5"),
                                    BinaryStringData.fromString("6"),
                                    BinaryStringData.fromString("7")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("7"),
                                    BinaryStringData.fromString("6"),
                                    BinaryStringData.fromString("5")
                                }));
        assertThat(mapper.readTree(serializationSchema.serialize(updateEvent)))
                .isEqualTo(mapper.readTree("{\"col2\":\"6\"}"));

        // Drop col1
        serializationSchema.serialize(
                new DropColumnEvent(TABLE_1, Collections.singletonList("col1")));
        generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TABLE_1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("20"),
                                    BinaryStringData.fromString("30")
                                }));
        assertThat(mapper.readTree(serializationSchema.serialize(insertEvent3)))
                .isEqualTo(mapper.readTree("{\"col2\":\"20\"}"));
    }
}
