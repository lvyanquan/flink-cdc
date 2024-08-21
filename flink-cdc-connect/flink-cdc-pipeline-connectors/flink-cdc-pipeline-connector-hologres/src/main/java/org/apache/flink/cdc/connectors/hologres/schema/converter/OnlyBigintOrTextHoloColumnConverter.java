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

package org.apache.flink.cdc.connectors.hologres.schema.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import com.alibaba.hologres.client.model.Column;

import java.sql.Types;

import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_CHARACTER_VARYING_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;

/**
 * Convert CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column} in
 * String or Bigint mode.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>others -> PG_TEXT
 */
@Internal
public class OnlyBigintOrTextHoloColumnConverter extends DataTypeDefaultVisitor<Column> {
    protected final boolean isPrimaryKey;

    protected final com.alibaba.hologres.client.model.Column holoColumn;

    public OnlyBigintOrTextHoloColumnConverter(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        holoColumn = new com.alibaba.hologres.client.model.Column();
        holoColumn.setPrimaryKey(isPrimaryKey);
        // holoColumn.setDefaultValue(false);
    }

    // -------------------------- TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT -----------------
    @Override
    public com.alibaba.hologres.client.model.Column visit(TinyIntType tinyIntType) {
        return visitAsBigInt(tinyIntType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(SmallIntType smallIntType) {
        return visitAsBigInt(smallIntType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(IntType intType) {
        return visitAsBigInt(intType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BigIntType bigIntType) {
        return visitAsBigInt(bigIntType);
    }

    // ------------------------ others -> PG_TEXT ------------------------------------------
    @Override
    public com.alibaba.hologres.client.model.Column visit(CharType charType) {
        return visitAsText(charType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(VarCharType varCharType) {
        return visitAsText(varCharType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BooleanType booleanType) {
        return visitAsText(booleanType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BinaryType binaryType) {
        return visitAsText(binaryType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(VarBinaryType bytesType) {
        return visitAsText(bytesType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DecimalType decimalType) {
        return visitAsText(decimalType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(FloatType floatType) {
        return visitAsText(floatType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DoubleType doubleType) {
        return visitAsText(doubleType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DateType dateType) {
        return visitAsText(dateType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(TimeType timeType) {
        return visitAsText(timeType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(TimestampType timestampType) {
        return visitAsText(timestampType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(ZonedTimestampType zonedTimestampType) {
        return visitAsText(zonedTimestampType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(
            LocalZonedTimestampType localZonedTimestampType) {
        return visitAsText(localZonedTimestampType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(ArrayType arrayType) {
        holoColumn.setAllowNull(arrayType.isNullable());
        holoColumn.setArrayType(true);
        holoColumn.setType(Types.ARRAY);
        String typeName;
        switch (arrayType.getElementType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                typeName = PG_BIGINT_ARRAY;
                break;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                typeName = PG_CHARACTER_VARYING_ARRAY;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type "
                                + arrayType.getElementType().getTypeRoot());
        }
        holoColumn.setTypeName(typeName);
        return holoColumn;
    }

    @Override
    protected com.alibaba.hologres.client.model.Column defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
    }

    private com.alibaba.hologres.client.model.Column visitAsBigInt(DataType dataType) {
        holoColumn.setAllowNull(dataType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.BIGINT);
        holoColumn.setTypeName(PG_BIGINT);
        return holoColumn;
    }

    private com.alibaba.hologres.client.model.Column visitAsText(DataType dataType) {
        holoColumn.setAllowNull(dataType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.VARCHAR);
        holoColumn.setTypeName(PG_TEXT);
        return holoColumn;
    }
}
