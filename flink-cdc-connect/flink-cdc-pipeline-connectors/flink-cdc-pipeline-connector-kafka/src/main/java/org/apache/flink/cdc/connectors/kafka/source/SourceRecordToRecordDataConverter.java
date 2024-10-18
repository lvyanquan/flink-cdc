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
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.SQL_TIME_FORMAT;

/**
 * Convert {@link SourceRecord} to {@link RecordData} according to {@link Schema} with possible type
 * normalization.
 */
public class SourceRecordToRecordDataConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Schema schema;
    private final TimestampFormat timestampFormat;

    public SourceRecordToRecordDataConverter(Schema schema, TimestampFormat timestampFormat) {
        this.schema = schema;
        this.timestampFormat = timestampFormat;
    }

    /** Convert RowData internal data to RecordData internal data. */
    public interface RowDataToRecordDataConverter extends Serializable {
        Object convert(Object obj, org.apache.flink.table.types.DataType dataType) throws Exception;
    }

    public RecordData convert(SourceRecord sourceRecord) throws Exception {
        return (RecordData)
                createConverter(schema.toRowDataType())
                        .convert(sourceRecord, sourceRecord.getSchema().toRowDataType());
    }

    private RowDataToRecordDataConverter createConverter(DataType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /** Creates a runtime converter which assuming input object is not null. */
    protected RowDataToRecordDataConverter createNotNullConverter(DataType type) {
        // if no matched user defined converter, fallback to the default converter
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return this::convertToByte;
            case SMALLINT:
                return this::convertToShort;
            case INTEGER:
                return this::convertToInt;
            case BIGINT:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToLocalTimeZoneTimestamp;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBinary;
            case DECIMAL:
                return new RowDataToRecordDataConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object obj, org.apache.flink.table.types.DataType dataType) {
                        return convertToDecimal((DecimalType) type, obj, dataType);
                    }
                };
            case ROW:
                return new RowDataToRecordDataConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(
                            Object obj, org.apache.flink.table.types.DataType dataType)
                            throws Exception {
                        return convertToRecord((RowType) type, obj, dataType);
                    }
                };
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    protected Boolean convertToBoolean(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof Byte) {
            return (byte) obj == 1;
        } else if (obj instanceof Short) {
            return (short) obj == 1;
        } else {
            return Boolean.parseBoolean(obj.toString());
        }
    }

    protected Byte convertToByte(Object obj, org.apache.flink.table.types.DataType dataType) {
        return Byte.parseByte(obj.toString());
    }

    protected Short convertToShort(Object obj, org.apache.flink.table.types.DataType dataType) {
        return Short.parseShort(obj.toString());
    }

    protected Integer convertToInt(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else {
            return Integer.parseInt(obj.toString());
        }
    }

    protected Long convertToLong(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Byte) {
            return ((Byte) obj).longValue();
        } else if (obj instanceof Integer) {
            return ((Integer) obj).longValue();
        } else if (obj instanceof Long) {
            return (Long) obj;
        } else {
            return Long.parseLong(obj.toString());
        }
    }

    protected Double convertToDouble(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof org.apache.flink.table.data.DecimalData) {
            org.apache.flink.table.data.DecimalData decimalData =
                    (org.apache.flink.table.data.DecimalData) obj;
            return decimalData.toBigDecimal().doubleValue();
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else if (obj instanceof Double) {
            return (Double) obj;
        } else {
            return Double.parseDouble(obj.toString());
        }
    }

    protected Float convertToFloat(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Float) {
            return (Float) obj;
        } else if (obj instanceof Double) {
            return ((Double) obj).floatValue();
        } else {
            return Float.parseFloat(obj.toString());
        }
    }

    protected Integer convertToDate(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        throw new IllegalArgumentException(
                "Unable to convert to DATE from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    protected Integer convertToTime(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        throw new IllegalArgumentException(
                "Unable to convert to TIME from unexpected value '"
                        + obj
                        + "' of type "
                        + obj.getClass().getName());
    }

    protected TimestampData convertToTimestamp(
            Object obj, org.apache.flink.table.types.DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case DATE:
                Integer days = (Integer) obj;
                return TimestampData.fromLocalDateTime(LocalDate.ofEpochDay(days).atStartOfDay());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                org.apache.flink.table.data.TimestampData timestampData =
                        (org.apache.flink.table.data.TimestampData) obj;
                return TimestampData.fromMillis(
                        timestampData.getMillisecond(), timestampData.getNanoOfMillisecond());
            default:
                throw new IllegalArgumentException(
                        "Unable to convert to TIMESTAMP from unexpected value '"
                                + obj
                                + "' of type "
                                + obj.getClass().getName());
        }
    }

    protected LocalZonedTimestampData convertToLocalTimeZoneTimestamp(
            Object obj, org.apache.flink.table.types.DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case DATE:
                Integer days = (Integer) obj;
                LocalDate date = LocalDate.ofEpochDay(days);
                return LocalZonedTimestampData.fromInstant(
                        date.atStartOfDay().toInstant(ZoneOffset.UTC));
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                org.apache.flink.table.data.TimestampData timestampData =
                        (org.apache.flink.table.data.TimestampData) obj;
                return LocalZonedTimestampData.fromEpochMillis(
                        timestampData.getMillisecond(), timestampData.getNanoOfMillisecond());
            default:
                throw new IllegalArgumentException(
                        "Unable to convert to TIMESTAMP_LTZ from unexpected value '"
                                + obj
                                + "' of type "
                                + obj.getClass().getName());
        }
    }

    protected BinaryStringData convertToString(
            Object obj, org.apache.flink.table.types.DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case DATE:
                int days = (int) obj;
                LocalDate date = LocalDate.ofEpochDay(days);
                return BinaryStringData.fromString(ISO_LOCAL_DATE.format(date));
            case TIME_WITHOUT_TIME_ZONE:
                int millisecond = (int) obj;
                LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
                return BinaryStringData.fromString(SQL_TIME_FORMAT.format(time));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                org.apache.flink.table.data.TimestampData timestamp =
                        (org.apache.flink.table.data.TimestampData) obj;
                switch (timestampFormat) {
                    case SQL:
                        return BinaryStringData.fromString(
                                SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                    case ISO_8601:
                        return BinaryStringData.fromString(
                                ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Unsupported timestamp format <%s>.", timestampFormat));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                org.apache.flink.table.data.TimestampData timestampWithLTZ =
                        (org.apache.flink.table.data.TimestampData) obj;
                switch (timestampFormat) {
                    case SQL:
                        return BinaryStringData.fromString(
                                SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                        timestampWithLTZ.toInstant().atOffset(ZoneOffset.UTC)));
                    case ISO_8601:
                        return BinaryStringData.fromString(
                                ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(
                                        timestampWithLTZ.toInstant().atOffset(ZoneOffset.UTC)));
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Unsupported timestamp format <%s>.", timestampFormat));
                }
            case BINARY:
            case VARBINARY:
                return BinaryStringData.fromBytes((byte[]) obj);
            default:
                return BinaryStringData.fromString(obj.toString());
        }
    }

    protected byte[] convertToBinary(Object obj, org.apache.flink.table.types.DataType dataType) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + obj.getClass().getSimpleName());
        }
    }

    protected DecimalData convertToDecimal(
            DecimalType decimalType, Object obj, org.apache.flink.table.types.DataType dataType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        switch (dataType.getLogicalType().getTypeRoot()) {
            case DECIMAL:
                org.apache.flink.table.data.DecimalData data =
                        (org.apache.flink.table.data.DecimalData) obj;
                return DecimalData.fromBigDecimal(data.toBigDecimal(), precision, scale);
            case BIGINT:
                Long longValue = (Long) obj;
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(longValue), precision, scale);
            case INTEGER:
                Integer intValue = (Integer) obj;
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(intValue), precision, scale);
            case SMALLINT:
                Short shortValue = (Short) obj;
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(shortValue), precision, scale);
            case TINYINT:
                Byte byteValue = (Byte) obj;
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(byteValue), precision, scale);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported DECIMAL value type: " + obj.getClass().getSimpleName());
        }
    }

    protected RecordData convertToRecord(
            RowType rowType, Object obj, org.apache.flink.table.types.DataType dataType)
            throws Exception {
        RowDataToRecordDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(DataField::getType)
                        .map(this::createConverter)
                        .toArray(RowDataToRecordDataConverter[]::new);
        String[] targetFieldNames = rowType.getFieldNames().toArray(new String[0]);
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        SourceRecord sourceRecord = (SourceRecord) obj;
        SchemaSpec schemaSpec = sourceRecord.getSchema();
        List<org.apache.flink.table.types.DataType> givenDataTypes =
                schemaSpec.getColumnDataTypes();

        int columns = schemaSpec.getColumnCount();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[columns];

        Map<String, Integer> columnIndex = new HashMap<>();
        for (int i = 0; i < columns; i++) {
            fieldGetters[i] = RowData.createFieldGetter(givenDataTypes.get(i).getLogicalType(), i);
            columnIndex.put(schemaSpec.getColumnNames().get(i), i);
        }

        int arity = targetFieldNames.length;
        Object[] fields = new Object[arity];
        for (int i = 0; i < arity; i++) {
            String fieldName = targetFieldNames[i];
            int index = columnIndex.getOrDefault(fieldName, -1);
            if (index < 0) {
                fields[i] = null;
            } else {
                Object fieldValue = fieldGetters[index].getFieldOrNull(sourceRecord.getRow());
                Object convertedFiled =
                        convertField(fieldConverters[i], fieldValue, givenDataTypes.get(index));
                fields[i] = convertedFiled;
            }
        }
        return generator.generate(fields);
    }

    private static Object convertField(
            RowDataToRecordDataConverter fieldConverter,
            Object fieldValue,
            org.apache.flink.table.types.DataType dataType)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, dataType);
        }
    }

    private static RowDataToRecordDataConverter wrapIntoNullableConverter(
            RowDataToRecordDataConverter converter) {
        return new RowDataToRecordDataConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object obj, org.apache.flink.table.types.DataType dataType)
                    throws Exception {
                if (obj == null) {
                    return null;
                }
                return converter.convert(obj, dataType);
            }
        };
    }
}
