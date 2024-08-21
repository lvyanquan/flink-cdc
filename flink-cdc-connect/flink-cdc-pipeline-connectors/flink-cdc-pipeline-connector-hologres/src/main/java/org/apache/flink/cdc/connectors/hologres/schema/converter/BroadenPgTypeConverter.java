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
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;

import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;

/**
 * Convert CDC {@link DataType} to Postgres(or hologres ) data type string in Normalization mode.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>CHAR、VARCHAR、STRING -> PG_TEXT FLOAT、
 *
 * <p>DOUBLE -> PG_DOUBLE_PRECISION
 */
@Internal
public class BroadenPgTypeConverter extends StandardPgTypeConverter {
    public BroadenPgTypeConverter(boolean isPrimaryKey) {
        super(isPrimaryKey);
    }

    @Override
    public String visit(TinyIntType tinyIntType) {
        return visitAsBigInt(tinyIntType);
    }

    @Override
    public String visit(SmallIntType smallIntType) {
        return visitAsBigInt(smallIntType);
    }

    @Override
    public String visit(IntType intType) {
        return visitAsBigInt(intType);
    }

    @Override
    public String visit(BigIntType bigIntType) {
        return visitAsBigInt(bigIntType);
    }

    @Override
    public String visit(CharType charType) {
        return visitAsText(charType);
    }

    @Override
    public String visit(VarCharType varCharType) {
        return visitAsText(varCharType);
    }

    @Override
    public String visit(FloatType floatType) {
        return visitAsDouble(floatType);
    }

    @Override
    public String visit(DoubleType doubleType) {
        return visitAsDouble(doubleType);
    }

    @Override
    public String visit(ArrayType arrayType) {
        switch (arrayType.getElementType().getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return String.format("%s[]", arrayType.getElementType().accept(this));
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type "
                                + arrayType.getElementType().getTypeRoot());
        }
    }

    private String visitAsBigInt(DataType dataType) {
        return PG_BIGINT;
    }

    private String visitAsText(DataType dataType) {
        return PG_TEXT;
    }

    private String visitAsDouble(DataType dataType) {
        return PG_DOUBLE_PRECISION;
    }
}
