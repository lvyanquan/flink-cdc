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
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.hologres.schema.transformer.StringOrBigintHoloColumnTransformer;
import org.apache.flink.cdc.connectors.hologres.schema.transformer.StringOrBigintPgTypeTransformer;

import com.alibaba.hologres.client.model.Column;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * Transforms CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column} or
 * hologres type in String or Bigint mode.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>others -> PG_TEXT
 */
@Internal
public class StringOrBigintTypeNormalizer extends NormalTypeNormalizer {
    private static final long serialVersionUID = 1L;

    @Override
    public String transformToHoloType(DataType dataType, boolean isPrimaryKey) {
        StringOrBigintPgTypeTransformer stringOrBigintPgTypeTransformer =
                new StringOrBigintPgTypeTransformer(isPrimaryKey);
        return dataType.accept(stringOrBigintPgTypeTransformer);
    }

    @Override
    public Column transformToHoloColumn(DataType dataType, boolean isPrimaryKey) {
        StringOrBigintHoloColumnTransformer stringOrBigintHoloColumnTransformer =
                new StringOrBigintHoloColumnTransformer(isPrimaryKey);
        return dataType.accept(stringOrBigintHoloColumnTransformer);
    }

    protected RecordData.FieldGetter createDataTypeToRecordFieldGetter(
            DataType fieldType, int fieldPos, boolean isPrimaryKey, ZoneId zoneId) {
        final RecordData.FieldGetter fieldGetter;
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
                // TINYINT will be mapped to PG_BIGINT.
                fieldGetter = (record) -> (short) record.getByte(fieldPos);
                break;
            case SMALLINT:
                // SMALLINT will be mapped to PG_BIGINT.
                fieldGetter = (record) -> record.getShort(fieldPos);
                break;
            case INTEGER:
                // INTEGER will be mapped to PG_BIGINT.
                fieldGetter = (record) -> record.getInt(fieldPos);
                break;
            case BIGINT:
                // BIGINT will be mapped to PG_BIGINT.
                fieldGetter = record -> record.getLong(fieldPos);
                break;
            case FLOAT:
                // FLOAT will be mapped to PG_TEXT.
                fieldGetter = record -> Float.toString(record.getFloat(fieldPos));
                break;
            case DOUBLE:
                // DOUBLE will be mapped to PG_TEXT.
                fieldGetter = record -> Double.toString(record.getDouble(fieldPos));
                break;
            case DECIMAL:
                // DECIMAL will be mapped to PG_NUMERIC if not as pk.
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                // DECIMAL will be mapped to PG_TEXT if as pk.
                fieldGetter =
                        record ->
                                record.getDecimal(fieldPos, decimalPrecision, decimalScale)
                                        .toString();
                break;
            case DATE:
                // DATE will be mapped to PG_TEXT.
                fieldGetter = record -> LocalDate.ofEpochDay(record.getInt(fieldPos)).toString();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to PG_TEXT.
                fieldGetter =
                        record ->
                                LocalTime.ofNanoOfDay(record.getInt(fieldPos) * 1_000_000L)
                                        .toString();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to PG_TEXT.
                fieldGetter =
                        record -> record.getTimestamp(fieldPos, getPrecision(fieldType)).toString();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // TIME_WITHOUT_TIME_ZONE will be mapped to PG_TEXT,(use PIPELINE_LOCAL_TIME_ZONE
                // rather system.default)
                fieldGetter =
                        record ->
                                ZonedDateTime.ofInstant(
                                                record.getLocalZonedTimestampData(
                                                                fieldPos, getPrecision(fieldType))
                                                        .toInstant(),
                                                zoneId)
                                        .toString();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                // TIMESTAMP_WITH_TIME_ZONE will be mapped to PG_TEXT.
                fieldGetter =
                        record ->
                                record.getZonedTimestamp(fieldPos, getPrecision(fieldType))
                                        .toString();
                break;
            case CHAR:
                // CHAR will be mapped to PG_TEXT.
            case VARCHAR:
                // VARCHAR will be mapped to PG_TEXT
                fieldGetter = record -> record.getString(fieldPos).toString();
                break;
            case BOOLEAN:
                // BOOLEAN will be mapped to PG_BOOLEAN
                fieldGetter = record -> Boolean.toString(record.getBoolean(fieldPos));
                break;
            case BINARY:
            case VARBINARY:
                // BINARY and VARBINARY will be mapped to PG_BYTEA
                fieldGetter = record -> String.valueOf(record.getBinary(fieldPos));
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
        throw new UnsupportedOperationException(
                "Hologres does not support array type " + elementType);
    }
}
