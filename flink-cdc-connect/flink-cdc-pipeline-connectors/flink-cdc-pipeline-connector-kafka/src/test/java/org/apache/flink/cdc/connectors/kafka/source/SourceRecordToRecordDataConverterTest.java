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

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SourceRecordToRecordDataConverter}. */
public class SourceRecordToRecordDataConverterTest {

    private final ObjectPath tablePath = new ObjectPath("test-db", "test-table");
    private final TimestampFormat timestampFormat = TimestampFormat.SQL;

    private final JsonToSourceRecordConverter jsonToSourceRecordConverter =
            new JsonToSourceRecordConverter(
                    tablePath,
                    new RowType(Collections.emptyList()),
                    new JsonToRowDataConverters(false, false, timestampFormat, ZoneOffset.UTC),
                    false,
                    false,
                    false);

    @ParameterizedTest
    @MethodSource("parameterProviderForTestConvert")
    public void testConvert(
            org.apache.flink.table.types.DataType flinkDataType,
            Object flinkValue,
            DataType cdcDataType,
            Object expectedValue)
            throws Exception {
        String fieldName = "f";
        Schema schema = Schema.newBuilder().physicalColumn(fieldName, cdcDataType).build();
        SourceRecordToRecordDataConverter converter =
                new SourceRecordToRecordDataConverter(schema, timestampFormat);
        GenericRowData rowData = GenericRowData.of(flinkValue);
        RowType rowType =
                (RowType)
                        org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                fieldName, flinkDataType))
                                .getLogicalType();
        SchemaSpec schemaSpec = SchemaSpec.fromRowType(rowType);
        SourceRecord sourceRecord = new SourceRecord(tablePath, schemaSpec, rowData);

        RecordData recordData = converter.convert(sourceRecord);
        assertThat(RecordData.createFieldGetter(cdcDataType, 0).getFieldOrNull(recordData))
                .isEqualTo(expectedValue);
    }

    private static Stream<Arguments> parameterProviderForTestConvert() {
        LocalDate date = LocalDate.of(2024, 10, 14);
        LocalDateTime dateTime = LocalDateTime.of(2024, 10, 14, 10, 30, 59);

        return Stream.of(
                // convert null
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(), null,
                        DataTypes.INT(), null),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(), null,
                        DataTypes.TINYINT(), null),
                // convert numeric
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(), (byte) 1,
                        DataTypes.TINYINT(), (byte) 1),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(), (byte) 1,
                        DataTypes.SMALLINT(), (short) 1),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(),
                        (byte) 1,
                        DataTypes.INT(),
                        1),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(),
                        (byte) 1,
                        DataTypes.BIGINT(),
                        1L),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(),
                        (byte) 1,
                        DataTypes.DECIMAL(8, 2),
                        DecimalData.fromBigDecimal(BigDecimal.ONE, 8, 2)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(),
                        (byte) 1,
                        DataTypes.FLOAT(),
                        1f),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TINYINT(),
                        (byte) 1,
                        DataTypes.DOUBLE(),
                        1d),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(), (short) 2,
                        DataTypes.SMALLINT(), (short) 2),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        (short) 2,
                        DataTypes.INT(),
                        2),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        (short) 2,
                        DataTypes.BIGINT(),
                        2L),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        (short) 2,
                        DataTypes.DECIMAL(8, 2),
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(2), 8, 2)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        (short) 2,
                        DataTypes.FLOAT(),
                        2f),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.SMALLINT(),
                        (short) 2,
                        DataTypes.DOUBLE(),
                        2d),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(), 3,
                        DataTypes.INT(), 3),
                Arguments.of(org.apache.flink.table.api.DataTypes.INT(), 3, DataTypes.BIGINT(), 3L),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(),
                        3,
                        DataTypes.DECIMAL(8, 2),
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(3), 8, 2)),
                Arguments.of(org.apache.flink.table.api.DataTypes.INT(), 3, DataTypes.FLOAT(), 3f),
                Arguments.of(org.apache.flink.table.api.DataTypes.INT(), 3, DataTypes.DOUBLE(), 3d),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BIGINT(), 4L,
                        DataTypes.BIGINT(), 4L),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BIGINT(),
                        4L,
                        DataTypes.DECIMAL(8, 2),
                        DecimalData.fromBigDecimal(BigDecimal.valueOf(4), 8, 2)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BIGINT(), 4L, DataTypes.FLOAT(), 4f),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BIGINT(), 4L, DataTypes.DOUBLE(), 4d),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DECIMAL(8, 2),
                                org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                        BigDecimal.valueOf(1234.56), 8, 2),
                        DataTypes.DECIMAL(8, 2),
                                DecimalData.fromBigDecimal(BigDecimal.valueOf(1234.56), 8, 2)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DECIMAL(8, 2),
                        org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                BigDecimal.valueOf(1234.56), 8, 2),
                        DataTypes.FLOAT(),
                        1234.56f),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DECIMAL(8, 2),
                        org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                BigDecimal.valueOf(1234.56), 8, 2),
                        DataTypes.DOUBLE(),
                        1234.56d),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.FLOAT(), 1234.567f,
                        DataTypes.FLOAT(), 1234.567f),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.FLOAT(),
                        1234.567f,
                        DataTypes.DOUBLE(),
                        ((Float) 1234.567f).doubleValue()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DOUBLE(), 1234.5678d,
                        DataTypes.DOUBLE(), 1234.5678d),
                // convert date/timestamp
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DATE(), (int) date.toEpochDay(),
                        DataTypes.DATE(), (int) date.toEpochDay()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DATE(),
                        (int) date.toEpochDay(),
                        DataTypes.TIMESTAMP(),
                        TimestampData.fromLocalDateTime(date.atStartOfDay())),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DATE(),
                        (int) date.toEpochDay(),
                        DataTypes.TIMESTAMP_LTZ(),
                        LocalZonedTimestampData.fromInstant(
                                date.atStartOfDay().toInstant(ZoneOffset.UTC))),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(),
                                org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                                        dateTime),
                        DataTypes.TIMESTAMP(), TimestampData.fromLocalDateTime(dateTime)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(),
                                org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                                        dateTime),
                        DataTypes.TIMESTAMP_LTZ(),
                                LocalZonedTimestampData.fromInstant(
                                        dateTime.toInstant(ZoneOffset.UTC))),
                // convert other types
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BOOLEAN(), true,
                        DataTypes.BOOLEAN(), true),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.CHAR(1), StringData.fromString("a"),
                        DataTypes.CHAR(1), BinaryStringData.fromString("a")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BINARY(3), new byte[] {1, 2, 3},
                        DataTypes.BINARY(3), new byte[] {1, 2, 3}),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIME(), 1000,
                        DataTypes.TIME(), 1000),
                // convert to string
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                                StringData.fromString("flink"),
                        DataTypes.STRING(), BinaryStringData.fromString("flink")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BOOLEAN(),
                        true,
                        DataTypes.STRING(),
                        BinaryStringData.fromString("true")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.BINARY(10),
                                "flink".getBytes(StandardCharsets.UTF_8),
                        DataTypes.STRING(), BinaryStringData.fromString("flink")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(),
                        123,
                        DataTypes.STRING(),
                        BinaryStringData.fromString("123")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DECIMAL(8, 2),
                                org.apache.flink.table.data.DecimalData.fromBigDecimal(
                                        BigDecimal.valueOf(1234.56), 8, 2),
                        DataTypes.STRING(), BinaryStringData.fromString("1234.56")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DOUBLE(),
                        1234.5678d,
                        DataTypes.STRING(),
                        BinaryStringData.fromString("1234.5678")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIME(),
                        LocalTime.of(23, 59, 59).toSecondOfDay() * 1000,
                        DataTypes.STRING(),
                        BinaryStringData.fromString("23:59:59")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.DATE(),
                        (int) date.toEpochDay(),
                        DataTypes.STRING(),
                        BinaryStringData.fromString("2024-10-14")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP(),
                                org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                                        dateTime),
                        DataTypes.STRING(), BinaryStringData.fromString("2024-10-14 10:30:59")),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ(),
                                org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                                        dateTime),
                        DataTypes.STRING(), BinaryStringData.fromString("2024-10-14 10:30:59Z")));
    }

    @ParameterizedTest
    @MethodSource("parameterProviderForTestIllegalConvert")
    public void testIllegalConvert(
            org.apache.flink.table.types.DataType flinkDataType,
            Object flinkValue,
            DataType cdcDataType) {
        String fieldName = "f";
        Schema schema = Schema.newBuilder().physicalColumn(fieldName, cdcDataType).build();
        SourceRecordToRecordDataConverter converter =
                new SourceRecordToRecordDataConverter(schema, timestampFormat);

        GenericRowData rowData = GenericRowData.of(flinkValue);
        RowType rowType =
                (RowType)
                        org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                fieldName, flinkDataType))
                                .getLogicalType();
        SchemaSpec schemaSpec = SchemaSpec.fromRowType(rowType);
        SourceRecord sourceRecord = new SourceRecord(tablePath, schemaSpec, rowData);

        assertThatThrownBy(() -> converter.convert(sourceRecord));
    }

    private static Stream<Arguments> parameterProviderForTestIllegalConvert() {
        return Stream.of(
                // convert value with wider type to narrow type
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.TINYINT()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.SMALLINT()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.INT()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.BIGINT()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.DECIMAL(8, 2)),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.FLOAT()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        StringData.fromString("str"),
                        DataTypes.DOUBLE()),
                // convert null value for notnull data type
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(),
                        null,
                        DataTypes.INT().notNull()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        null,
                        DataTypes.STRING().notNull()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.STRING(),
                        null,
                        DataTypes.INT().notNull()),
                Arguments.of(
                        org.apache.flink.table.api.DataTypes.INT(),
                        null,
                        DataTypes.STRING().notNull()));
    }

    @Test
    public void testConvertFromJsonToString() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("f0", DataTypes.STRING())
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f2", DataTypes.STRING())
                        .physicalColumn("f3", DataTypes.STRING())
                        .physicalColumn("f4", DataTypes.STRING())
                        .physicalColumn("f5", DataTypes.STRING())
                        .physicalColumn("f6", DataTypes.STRING())
                        .physicalColumn("f7", DataTypes.STRING())
                        .physicalColumn("f8", DataTypes.STRING())
                        .build();
        SourceRecordToRecordDataConverter converter =
                new SourceRecordToRecordDataConverter(schema, timestampFormat);

        String json =
                "{"
                        + "    \"f0\": true,"
                        + "    \"f1\": 23,"
                        + "    \"f2\": 34.23,"
                        + "    \"f3\": \"2024-10-12\","
                        + "    \"f4\": \"00:00:30\","
                        + "    \"f5\": \"2024-10-12 22:00:30\","
                        + "    \"f6\": \"2024-10-12 22:00:30Z\","
                        + "    \"f7\": [1, 2, 3],"
                        + "    \"f8\": {\"nested\": \"value\"}"
                        + "}";
        SourceRecord sourceRecord = getSourceRecordByJson(json);

        RecordData recordData = converter.convert(sourceRecord);
        assertThat(recordData.getString(0).toString()).isEqualTo("true");
        assertThat(recordData.getString(1).toString()).isEqualTo("23");
        assertThat(recordData.getString(2).toString()).isEqualTo("34.23");
        assertThat(recordData.getString(3).toString()).isEqualTo("2024-10-12");
        assertThat(recordData.getString(4).toString()).isEqualTo("00:00:30");
        assertThat(recordData.getString(5).toString()).isEqualTo("2024-10-12 22:00:30");
        assertThat(recordData.getString(6).toString()).isEqualTo("2024-10-12 22:00:30Z");
        assertThat(recordData.getString(7).toString()).isEqualTo("[1,2,3]");
        assertThat(recordData.getString(8).toString()).isEqualTo("{\"nested\":\"value\"}");
    }

    @Test
    public void testConvertFromJson() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("bool", DataTypes.BOOLEAN())
                        .physicalColumn("bigint", DataTypes.BIGINT())
                        .physicalColumn("double", DataTypes.DOUBLE())
                        .physicalColumn("string", DataTypes.STRING())
                        .physicalColumn("date", DataTypes.DATE())
                        .physicalColumn("time", DataTypes.TIME())
                        .physicalColumn("timestamp", DataTypes.TIMESTAMP(3))
                        .physicalColumn("timestamp_ltz", DataTypes.TIMESTAMP_LTZ(3))
                        .build();
        SourceRecordToRecordDataConverter converter =
                new SourceRecordToRecordDataConverter(schema, timestampFormat);

        String json =
                "{"
                        + "    \"bool\": true,"
                        + "    \"bigint\": 23,"
                        + "    \"double\": 34.23,"
                        + "    \"string\": \"string value\","
                        + "    \"date\": \"1970-01-02\","
                        + "    \"time\": \"00:00:30\","
                        + "    \"timestamp\": \"2024-10-12 22:00:30.111\","
                        + "    \"timestamp_ltz\": \"1970-01-02 00:00:30.111Z\""
                        + "}";
        SourceRecord sourceRecord = getSourceRecordByJson(json);

        RecordData recordData = converter.convert(sourceRecord);
        assertThat(recordData.getBoolean(0)).isEqualTo(true);
        assertThat(recordData.getLong(1)).isEqualTo(23L);
        assertThat(recordData.getDouble(2)).isEqualTo(34.23);
        assertThat(recordData.getString(3).toString()).isEqualTo("string value");
        assertThat(recordData.getInt(4)).isEqualTo(1);
        assertThat(recordData.getInt(5)).isEqualTo(30000);
        assertThat(recordData.getTimestamp(6, 3))
                .isEqualTo(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2024, 10, 12, 22, 0, 30, 111000000)));
        assertThat(recordData.getLocalZonedTimestampData(7, 3))
                .isEqualTo(LocalZonedTimestampData.fromEpochMillis(86430111));
    }

    private SourceRecord getSourceRecordByJson(String json) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonParser root = objectMapper.getFactory().createParser(json);
        if (root.currentToken() == null) {
            root.nextToken();
        }
        return jsonToSourceRecordConverter.convert(root);
    }
}
