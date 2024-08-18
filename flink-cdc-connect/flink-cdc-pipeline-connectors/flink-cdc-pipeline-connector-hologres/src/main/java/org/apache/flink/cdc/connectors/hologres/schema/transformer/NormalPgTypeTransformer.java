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
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BOOLEAN;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_BYTEA;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_CHAR;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_CHARACTER_VARYING;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DATE;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_DOUBLE_PRECISION;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_INTEGER;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_NUMERIC;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_REAL;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_SMALLINT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TEXT;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIME;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMP;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMPTZ;
import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_VARCHAR_MAX_SIZE;

/** Transforms CDC {@link DataType} to Postgres(or hologres ) data type string. */
@Internal
public class NormalPgTypeTransformer extends DataTypeDefaultVisitor<String> {

    private final boolean isPrimaryKey;

    public NormalPgTypeTransformer(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    @Override
    public String visit(CharType charType) {
        return String.format("%s(%s)", PG_CHAR, charType.getLength());
    }

    @Override
    public String visit(VarCharType varCharType) {
        if (varCharType.getLength() > PG_VARCHAR_MAX_SIZE) {
            return PG_TEXT;
        } else {
            return String.format("%s(%s)", PG_CHARACTER_VARYING, varCharType.getLength());
        }
    }

    @Override
    public String visit(BooleanType booleanType) {
        return PG_BOOLEAN;
    }

    @Override
    public String visit(BinaryType binaryType) {
        // BYTEA is variable-length binary string
        // BYTEA doesn't support length
        return PG_BYTEA;
    }

    @Override
    public String visit(VarBinaryType bytesType) {
        // BYTEA is variable-length binary string
        // BYTEA doesn't support length
        return PG_BYTEA;
    }

    @Override
    public String visit(DecimalType decimalType) {
        // Hologres does not support Decimal as primary key, so decimal should be cast to
        // VARCHAR（ PG_VARCHAR_MAX_SIZE is enough for Decimal precision must be between 1 and 38
        // )

        if (!isPrimaryKey) {
            return String.format(
                    "%s(%s, %s)", PG_NUMERIC, decimalType.getPrecision(), decimalType.getScale());
        }
        // For a DecimalType with precision N, we may need N + 1 or N + 2 characters to
        // store it as a
        // string (one for negative sign, and one for decimal point)
        int length =
                decimalType.getScale() != 0
                        ? decimalType.getPrecision() + 2
                        : decimalType.getPrecision() + 1;

        return String.format("%s(%s)", PG_CHARACTER_VARYING, length);
    }

    @Override
    public String visit(TinyIntType tinyIntType) {
        // hologres not support tinyint
        return PG_SMALLINT;
    }

    @Override
    public String visit(SmallIntType smallIntType) {
        return PG_SMALLINT;
    }

    @Override
    public String visit(IntType intType) {
        return PG_INTEGER;
    }

    @Override
    public String visit(BigIntType bigIntType) {
        return PG_BIGINT;
    }

    @Override
    public String visit(FloatType floatType) {
        return PG_REAL;
    }

    @Override
    public String visit(DoubleType doubleType) {
        return PG_DOUBLE_PRECISION;
    }

    @Override
    public String visit(DateType dateType) {
        return PG_DATE;
    }

    @Override
    public String visit(TimeType timeType) {
        return PG_TIME;
    }

    @Override
    public String visit(TimestampType timestampType) {
        return PG_TIMESTAMP;
    }

    @Override
    public String visit(ZonedTimestampType zonedTimestampType) {
        // TIMESTAMPTZ type doesn't support precision now, use holo timestamptz by default
        return PG_TIMESTAMPTZ;
    }

    @Override
    public String visit(LocalZonedTimestampType localZonedTimestampType) {
        // TIMESTAMPTZ type doesn't support precision now, use holo timestamptz by default
        return PG_TIMESTAMPTZ;
    }

    @Override
    public String visit(ArrayType arrayType) {
        switch (arrayType.getElementType().getTypeRoot()) {
            case BOOLEAN:
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

    @Override
    protected String defaultMethod(DataType dataType) {
        throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
    }
}
