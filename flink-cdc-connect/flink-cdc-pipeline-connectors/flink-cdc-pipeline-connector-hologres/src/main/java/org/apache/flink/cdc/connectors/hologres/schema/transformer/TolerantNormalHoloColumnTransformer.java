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
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarCharType;

import java.sql.Types;

import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BIGINT_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BOOLEAN_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION_ARRAY;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT_ARRAY;

/**
 * Transforms CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column} in
 * Normalization mode.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>CHAR、VARCHAR、STRING -> PG_TEXT
 *
 * <p>FLOAT、DOUBLE -> PG_DOUBLE_PRECISION
 */
@Internal
public class TolerantNormalHoloColumnTransformer extends NormalHoloColumnTransformer {
    public TolerantNormalHoloColumnTransformer(boolean isPrimaryKey) {
        super(isPrimaryKey);
    }

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

    @Override
    public com.alibaba.hologres.client.model.Column visit(CharType charType) {
        return visitAsText(charType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(VarCharType varCharType) {
        return visitAsText(varCharType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(FloatType floatType) {
        return visitAsDouble(floatType);
    }

    @Override
    public com.alibaba.hologres.client.model.Column visit(DoubleType doubleType) {
        return visitAsDouble(doubleType);
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
                typeName = PG_DOUBLE_PRECISION_ARRAY;
                break;
            case BOOLEAN:
                typeName = PG_BOOLEAN_ARRAY;
                break;
            case CHAR:
            case VARCHAR:
                typeName = PG_TEXT_ARRAY;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Hologres does not support array type "
                                + arrayType.getElementType().getTypeRoot());
        }
        holoColumn.setTypeName(typeName);
        return holoColumn;
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

    private com.alibaba.hologres.client.model.Column visitAsDouble(DataType dataType) {
        holoColumn.setAllowNull(dataType.isNullable());
        holoColumn.setArrayType(false);
        holoColumn.setType(Types.DOUBLE);
        holoColumn.setTypeName(PG_DOUBLE_PRECISION);
        return holoColumn;
    }
}
