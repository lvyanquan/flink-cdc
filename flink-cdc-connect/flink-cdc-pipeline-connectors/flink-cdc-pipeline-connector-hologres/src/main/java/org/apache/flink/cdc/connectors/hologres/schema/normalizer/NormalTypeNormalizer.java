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

package org.apache.flink.cdc.connectors.hologres.schema.normalizer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.hologres.schema.transformer.NormalHoloColumnTransformer;
import org.apache.flink.cdc.connectors.hologres.schema.transformer.NormalPgTypeTransformer;

import com.alibaba.hologres.client.model.Column;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** Transforms CDC {@link DataType} to Hologres Type in normal type normalize mode. */
@Internal
public class NormalTypeNormalizer implements HologresTypeNormalizer {
    private static final long serialVersionUID = 1L;

    public NormalTypeNormalizer() {}

    @Override
    public String transformToHoloType(DataType dataType, boolean isPrimaryKey) {
        NormalPgTypeTransformer normalPgTypeTransformer = new NormalPgTypeTransformer(isPrimaryKey);
        return dataType.accept(normalPgTypeTransformer);
    }

    @Override
    public Column transformToHoloColumn(DataType dataType, boolean isPrimaryKey) {
        NormalHoloColumnTransformer holoColumnTransformer =
                new NormalHoloColumnTransformer(isPrimaryKey);
        return dataType.accept(holoColumnTransformer);
    }

    @Override
    public RecordData.FieldGetter[] createFieldGetters(Schema schema, ZoneId zoneId) {
        RecordData.FieldGetter[] fieldGetters = new RecordData.FieldGetter[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            org.apache.flink.cdc.common.schema.Column column = schema.getColumns().get(i);
            fieldGetters[i] =
                    createDataTypeToRecordFieldGetter(
                            column.getType(),
                            i,
                            schema.primaryKeys().contains(column.getName()),
                            zoneId);
        }

        return fieldGetters;
    }

    protected RecordData.FieldGetter createDataTypeToRecordFieldGetter(
            DataType fieldType, int fieldPos, boolean isPrimaryKey, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
                // TINYINT will be mapped to PG_SMALLINT.
                fieldGetter = (record) -> (short) record.getByte(fieldPos);
                break;
            case SMALLINT:
                // SMALLINT will be mapped to PG_SMALLINT.
                fieldGetter = (record) -> record.getShort(fieldPos);
                break;
            case INTEGER:
                // INTEGER will be mapped to PG_INTEGER.
                fieldGetter = (record) -> record.getInt(fieldPos);
                break;
            case BIGINT:
                // BIGINT will be mapped to PG_BIGINT.
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                // FLOAT will be mapped to PG_DOUBLE_PRECISION.
                fieldGetter = record -> record.getFloat(fieldPos);
                break;
            case DOUBLE:
                // DOUBLE will be mapped to PG_REAL.
                fieldGetter = record -> record.getDouble(fieldPos);
                break;
            case DECIMAL:
                // DECIMAL will be mapped to PG_NUMERIC if not as pk.
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                if (!isPrimaryKey) {
                    fieldGetter =
                            record ->
                                    record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                            .toBigDecimal();
                } else {
                    // DECIMAL will be mapped to PG_VARCHAR if as pk.
                    fieldGetter =
                            record ->
                                    record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                            .toString();
                }
                break;
            case DATE:
                // DATE will be mapped to PG_DATE.
                fieldGetter = record -> Date.valueOf(LocalDate.ofEpochDay(record.getInt(fieldPos)));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to PG_TIME.
                fieldGetter =
                        record ->
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(
                                                record.getInt(fieldPos) * 1_000_000L));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to PG_TIMESTAMP.
                fieldGetter =
                        record ->
                                record.getTimestamp(fieldPos, getPrecision(fieldType))
                                        .toTimestamp();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to TIMESTAMP_WITH_LOCAL_TIME_ZONE.
                fieldGetter =
                        record ->
                                Timestamp.from(
                                        record.getLocalZonedTimestampData(
                                                        fieldPos, getPrecision(fieldType))
                                                .toInstant());
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                // TIMESTAMP_WITH_TIME_ZONE will be mapped to TIMESTAMP_WITH_LOCAL_TIME_ZONE.
                fieldGetter =
                        record ->
                                record.getZonedTimestamp(fieldPos, getPrecision(fieldType))
                                        .toTimestamp();
                break;
            case CHAR:
                // CHAR will be mapped to PG_CHAR.
            case VARCHAR:
                // VARCHAR will be mapped to PG_TEXT or PG_CHARACTER_VARYING
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case BOOLEAN:
                // BOOLEAN will be mapped to PG_BOOLEAN
                fieldGetter = record -> record.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                // BINARY and VARBINARY will be mapped to PG_BYTEA
                fieldGetter = record -> record.getBinary(fieldPos);
                break;
            case ARRAY:
                fieldGetter =
                        record ->
                                getElementsFromArrayData(
                                        ((ArrayType) fieldType).getElementType(),
                                        record.getArray(fieldPos));
                break;
            default:
                // MAP, MULTISET, ROW, RAW are not supported
                throw new UnsupportedOperationException(
                        String.format(
                                "Hologres doesn't support to create tables with type: %s.",
                                fieldType));
        }

        return (record) -> {
            if (record == null) {
                return null;
            } else {
                return fieldGetter.getFieldOrNull(record);
            }
        };
    }

    static Object getElementsFromArrayData(DataType elementType, ArrayData arrayData) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return arrayData.toBooleanArray();
            case INTEGER:
                return arrayData.toIntArray();
            case BIGINT:
                return arrayData.toLongArray();
            case FLOAT:
                return arrayData.toFloatArray();
            case DOUBLE:
                return arrayData.toDoubleArray();
            case CHAR:
            case VARCHAR:
                String[] strings = new String[arrayData.size()];
                for (int i = 0; i < arrayData.size(); i++) {
                    strings[i] = arrayData.getString(i).toString();
                }
                return strings;
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type " + elementType);
        }
    }
}
