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

package org.apache.flink.cdc.connectors.hologres.schema.transformer;

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

import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;

/**
 * Transforms CDC {@link DataType} to to Postgres(or hologres ) data type string.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>others -> PG_TEXT.
 */
@Internal
public class StringOrBigintPgTypeTransformer extends DataTypeDefaultVisitor<String> {

    private final boolean isPrimaryKey;

    public StringOrBigintPgTypeTransformer(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    // -------------------------- TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT -----------------

    @Override
    public String visit(TinyIntType tinyIntType) {
        return PG_BIGINT;
    }

    @Override
    public String visit(SmallIntType smallIntType) {
        return PG_BIGINT;
    }

    @Override
    public String visit(IntType intType) {
        return PG_BIGINT;
    }

    @Override
    public String visit(BigIntType bigIntType) {
        return PG_BIGINT;
    }

    // ------------------------ others -> PG_TEXT ------------------------------------------

    @Override
    public String visit(FloatType floatType) {
        return PG_TEXT;
    }

    @Override
    public String visit(CharType charType) {
        return PG_TEXT;
    }

    @Override
    public String visit(VarCharType varCharType) {
        return PG_TEXT;
    }

    @Override
    public String visit(BooleanType booleanType) {
        return PG_TEXT;
    }

    @Override
    public String visit(BinaryType binaryType) {
        return PG_TEXT;
    }

    @Override
    public String visit(VarBinaryType bytesType) {
        return PG_TEXT;
    }

    @Override
    public String visit(DecimalType decimalType) {
        return PG_TEXT;
    }

    @Override
    public String visit(DoubleType doubleType) {
        return PG_TEXT;
    }

    @Override
    public String visit(DateType dateType) {
        return PG_TEXT;
    }

    @Override
    public String visit(TimeType timeType) {
        return PG_TEXT;
    }

    @Override
    public String visit(TimestampType timestampType) {
        return PG_TEXT;
    }

    @Override
    public String visit(ZonedTimestampType zonedTimestampType) {
        return PG_TEXT;
    }

    @Override
    public String visit(LocalZonedTimestampType localZonedTimestampType) {
        return PG_TEXT;
    }

    @Override
    public String visit(ArrayType arrayType) {
        switch (arrayType.getElementType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
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
                return String.format("%s[]", arrayType.getElementType().accept(this));
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type "
                                + arrayType.getElementType().getTypeRoot());
        }
    }

    @Override
    protected String defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
    }
}
