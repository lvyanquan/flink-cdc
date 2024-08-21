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
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BOOLEAN;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BOOLEAN_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BYTEA;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_CHARACTER_VARYING;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_CHARACTER_VARYING_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DATE;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_INTEGER;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_INTEGER_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_NUMERIC;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_REAL;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_REAL_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_SMALLINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIME;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMP;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMPTZ;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMP_TIME_PRECISION;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_VARCHAR_MAX_SIZE;

/** Convert CDC {@link DataType} to Postgres(or hologres ) data type string. */
@Internal
public class StandardHoloColumnConverter extends DataTypeDefaultVisitor<Column> {
    protected final boolean isPrimaryKey;

    protected final com.alibaba.hologres.client.model.Column holoColumn;

    public StandardHoloColumnConverter(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        holoColumn = new com.alibaba.hologres.client.model.Column();
        holoColumn.setPrimaryKey(isPrimaryKey);
        // holoColumn.setDefaultValue(false);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(CharType charType) {
        holoColumn.setAllowNull(charType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.VARCHAR);
        holoColumn.setTypeName(PG_CHARACTER_VARYING);
        holoColumn.setPrecision(charType.getLength());
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(VarCharType varCharType) {
        holoColumn.setAllowNull(varCharType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.VARCHAR);
        if (varCharType.getLength() <= PG_VARCHAR_MAX_SIZE) {
            holoColumn.setTypeName(PG_CHARACTER_VARYING);
        } else {
            holoColumn.setTypeName(PG_TEXT);
        }
        holoColumn.setPrecision(varCharType.getLength());
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BooleanType booleanType) {
        // PG_BOOLEAN -> Types.BIT
        holoColumn.setAllowNull(booleanType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.BIT);
        holoColumn.setTypeName(PG_BOOLEAN);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BinaryType binaryType) {
        // PG_BYTEA will be mapped to Types.BINARY
        holoColumn.setAllowNull(binaryType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.BINARY);
        holoColumn.setTypeName(PG_BYTEA);
        holoColumn.setPrecision(binaryType.getLength());
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(VarBinaryType bytesType) {
        // PG_BYTEA will be mapped to Types.BINARY
        holoColumn.setAllowNull(bytesType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.BINARY);
        holoColumn.setTypeName(PG_BYTEA);
        holoColumn.setPrecision(bytesType.getLength());
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DecimalType decimalType) {
        // Hologres does not support Decimal as primary key, so decimal should be cast to
        // VARCHAR.

        if (!isPrimaryKey) {
            holoColumn.setType(Types.NUMERIC);
            holoColumn.setTypeName(PG_NUMERIC);
            holoColumn.setScale(decimalType.getScale());
            holoColumn.setPrecision(decimalType.getPrecision());
        } else {
            // For a DecimalType with precision N, we may need N + 1 or N + 2 characters to
            // store it as a string (one for negative sign, and one for decimal point)
            int length =
                    decimalType.getScale() != 0
                            ? decimalType.getPrecision() + 2
                            : decimalType.getPrecision() + 1;
            holoColumn.setType(Types.VARCHAR);
            holoColumn.setTypeName(PG_CHARACTER_VARYING);
            holoColumn.setPrecision(length);
        }

        holoColumn.setAllowNull(decimalType.isNullable());
        holoColumn.setArrayType(false);

        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(TinyIntType tinyIntType) {
        holoColumn.setAllowNull(tinyIntType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.SMALLINT);
        holoColumn.setTypeName(PG_SMALLINT);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(SmallIntType smallIntType) {
        holoColumn.setAllowNull(smallIntType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.SMALLINT);
        holoColumn.setTypeName(PG_SMALLINT);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(IntType intType) {
        holoColumn.setAllowNull(intType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.INTEGER);
        holoColumn.setTypeName(PG_INTEGER);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(BigIntType bigIntType) {
        holoColumn.setAllowNull(bigIntType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.BIGINT);
        holoColumn.setTypeName(PG_BIGINT);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(FloatType floatType) {
        holoColumn.setAllowNull(floatType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.REAL);
        holoColumn.setTypeName(PG_REAL);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DoubleType doubleType) {
        holoColumn.setAllowNull(doubleType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.DOUBLE);
        holoColumn.setTypeName(PG_DOUBLE_PRECISION);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DateType dateType) {
        holoColumn.setAllowNull(dateType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.DATE);
        holoColumn.setTypeName(PG_DATE);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(TimeType timeType) {
        holoColumn.setAllowNull(timeType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.TIME);
        holoColumn.setTypeName(PG_TIME);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(TimestampType timestampType) {
        holoColumn.setAllowNull(timestampType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.TIMESTAMP);
        holoColumn.setTypeName(PG_TIMESTAMP);
        holoColumn.setPrecision(PG_TIMESTAMP_TIME_PRECISION);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(ZonedTimestampType zonedTimestampType) {
        holoColumn.setAllowNull(zonedTimestampType.isNullable());
        holoColumn.setArrayType(false);
        // PG_TIMESTAMPTZ will be mapping to Types.TIMESTAMP
        holoColumn.setType(Types.TIMESTAMP);
        holoColumn.setTypeName(PG_TIMESTAMPTZ);
        holoColumn.setPrecision(PG_TIMESTAMP_TIME_PRECISION);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(
            LocalZonedTimestampType localZonedTimestampType) {
        holoColumn.setAllowNull(localZonedTimestampType.isNullable());
        holoColumn.setArrayType(false);
        // PG_TIMESTAMPTZ will be mapping to Types.TIMESTAMP
        holoColumn.setType(Types.TIMESTAMP);
        holoColumn.setTypeName(PG_TIMESTAMPTZ);
        holoColumn.setPrecision(PG_TIMESTAMP_TIME_PRECISION);
        return holoColumn;
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(ArrayType arrayType) {
        holoColumn.setAllowNull(arrayType.isNullable());
        holoColumn.setArrayType(true);
        holoColumn.setType(Types.ARRAY);
        String typeName;
        switch (arrayType.getElementType().getTypeRoot()) {
            case INTEGER:
                typeName = PG_INTEGER_ARRAY;
                break;
            case BIGINT:
                typeName = PG_BIGINT_ARRAY;
                break;
            case FLOAT:
                typeName = PG_REAL_ARRAY;
                break;
            case DOUBLE:
                typeName = PG_DOUBLE_PRECISION_ARRAY;
                break;
            case BOOLEAN:
                typeName = PG_BOOLEAN_ARRAY;
                break;
            case CHAR:
            case VARCHAR:
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
}
