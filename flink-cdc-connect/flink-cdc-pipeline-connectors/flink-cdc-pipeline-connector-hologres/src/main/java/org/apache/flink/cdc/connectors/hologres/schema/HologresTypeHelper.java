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

package org.apache.flink.cdc.connectors.hologres.schema;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
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
import org.apache.flink.cdc.common.utils.StringUtils;

import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/**
 * A util helper to transform between CDC data types, Hologers/Postgres types, and jdbc Types). CDC
 * DataType | Postgres Type | Jdbc Types PG_CHAR
 */
public class HologresTypeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HologresTypeHelper.class);
    public static final String PG_SERIAL = "serial";
    public static final String PG_BIGSERIAL = "bigserial";
    public static final String PG_BYTEA = "bytea";
    public static final String PG_BYTEA_ARRAY = "_bytea";
    public static final String PG_ROARING_BITMAP = "roaringbitmap";
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLINT_ARRAY = "_int2";
    public static final String PG_INTEGER = "int4";
    public static final String PG_INTEGER_ARRAY = "_int4";
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGINT_ARRAY = "_int8";
    public static final String PG_REAL = "float4";
    public static final String PG_REAL_ARRAY = "_float4";
    public static final String PG_DOUBLE_PRECISION = "float8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String PG_NUMERIC = "numeric";
    public static final String PG_NUMERIC_ARRAY = "_numeric";
    public static final String PG_BOOLEAN = "bool";
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_TIMESTAMP = "timestamp";
    public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    public static final String PG_TIMESTAMPTZ = "timestamptz";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    public static final String PG_DATE = "date";
    public static final String PG_DATE_ARRAY = "_date";
    public static final String PG_TIME = "time";
    public static final String PG_TIME_ARRAY = "_time";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_CHAR = "bpchar";
    public static final String PG_CHAR_ARRAY = "_bpchar";
    public static final String PG_CHARACTER = "character";
    public static final String PG_CHARACTER_ARRAY = "_character";
    public static final String PG_CHARACTER_VARYING = "varchar";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    public static final String PG_JSON = "json";
    public static final String PG_JSONB = "jsonb";
    public static final String PG_GEOGRAPHY = "geography";
    public static final String PG_GEOMETRY = "geometry";
    public static final String PG_BOX2D = "box2d";
    public static final String PG_BOX3D = "box3d";
    public static final int PG_VARCHAR_MAX_SIZE = 10485760;
    public static final int PG_TIMESTAMP_TIME_PRECISION = 6;

    public static TableSchema inferTableSchema(
            Schema schema,
            TableId tableId,
            @Nullable String shardKeys,
            boolean enableTypeNormalization) {
        TableSchema.Builder builder = new TableSchema.Builder();
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<Column> columns = schema.getColumns();

        builder.setTableName(TableName.valueOf(tableId.getSchemaName(), tableId.getTableName()));
        builder.setComment(schema.comment());

        builder.setColumns(
                columns.stream()
                        .map(
                                column ->
                                        inferColumn(
                                                column,
                                                primaryKeys.contains(column.getName()),
                                                enableTypeNormalization))
                        .collect(Collectors.toList()));

        if (!partitionKeys.isEmpty()) {
            String partitionKey = partitionKeys.get(0);
            builder.setPartitionColumnName(partitionKey);
        }

        // Set default distribution keys as primary keys to write same pk value to same shard.
        if (StringUtils.isNullOrWhitespaceOnly(shardKeys)) {
            builder.setDistributionKeys(primaryKeys.toArray(new String[0]));
        } else {
            builder.setDistributionKeys(shardKeys.split(","));
        }

        TableSchema tableSchema = builder.build();
        tableSchema.calculateProperties();
        return tableSchema;
    }

    public static com.alibaba.hologres.client.model.Column inferColumn(
            Column column, boolean isPrimaryKey, boolean enableTypeNormalization) {
        com.alibaba.hologres.client.model.Column holoColumn =
                inferColumn(column.getType(), isPrimaryKey, enableTypeNormalization);
        holoColumn.setName(column.getName());
        holoColumn.setComment(column.getComment());
        return holoColumn;
    }

    public static com.alibaba.hologres.client.model.Column inferColumn(
            DataType dataType, boolean isPrimaryKey, boolean enableTypeNormalization) {
        HoloColumnTransformer holoColumnTransformer =
                enableTypeNormalization
                        ? new HoloColumnNormalizationTransformer(isPrimaryKey)
                        : new HoloColumnTransformer(isPrimaryKey);
        return dataType.accept(holoColumnTransformer);
    }

    public static String toPostgresType(
            DataType dataType, boolean isPrimaryKey, boolean enableTypeNormalization) {
        validatePrecision(dataType);
        PgTypeTransformer cdcDataTypeTransformer =
                enableTypeNormalization
                        ? new PgTypeNormalizationTransformer(isPrimaryKey)
                        : new PgTypeTransformer(isPrimaryKey);
        String jdbcType = dataType.accept(cdcDataTypeTransformer);
        if (!dataType.isNullable()) {
            return String.format("%s NOT NULL", jdbcType);
        } else {
            return jdbcType;
        }
    }

    public static String toNullablePostgresType(
            DataType dataType, boolean isPrimaryKey, boolean enableTypeNormalization) {
        validatePrecision(dataType);
        PgTypeTransformer cdcDataTypeTransformer =
                enableTypeNormalization
                        ? new PgTypeNormalizationTransformer(isPrimaryKey)
                        : new PgTypeTransformer(isPrimaryKey);
        return dataType.accept(cdcDataTypeTransformer);
    }

    private static void validatePrecision(DataType dataType) {
        int precision;
        switch (dataType.getTypeRoot()) {
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((TimeType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                precision = ((TimestampType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = ((LocalZonedTimestampType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                precision = ((ZonedTimestampType) dataType).getPrecision();
                break;
            default:
                return;
        }

        if (precision > PG_TIMESTAMP_TIME_PRECISION) {
            LOG.warn(
                    "Hologres only supports the precision is {} for {} but the specified precision is {}. "
                            + "When writing to the hologres with larger precision, the sink will CAST the data with the supported precision. "
                            + "To keep the precision, please CAST the value to STRING type if cares.",
                    PG_TIMESTAMP_TIME_PRECISION,
                    dataType.getTypeRoot(),
                    precision);
        }
    }

    /** Transforms CDC {@link DataType} to Postgres(or hologres ) data type string. */
    private static class PgTypeTransformer extends DataTypeDefaultVisitor<String> {

        private final boolean isPrimaryKey;

        public PgTypeTransformer(boolean isPrimaryKey) {
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
                        "%s(%s, %s)",
                        PG_NUMERIC, decimalType.getPrecision(), decimalType.getScale());
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

    /**
     * Transforms CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column}.
     */
    public static class HoloColumnTransformer
            extends DataTypeDefaultVisitor<com.alibaba.hologres.client.model.Column> {
        protected final boolean isPrimaryKey;

        protected final com.alibaba.hologres.client.model.Column holoColumn;

        public HoloColumnTransformer(boolean isPrimaryKey) {
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
        public com.alibaba.hologres.client.model.Column visit(
                ZonedTimestampType zonedTimestampType) {
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

    /**
     * Transforms CDC {@link DataType} to Postgres(or hologres ) data type string in Normalization
     * mode. TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT CHAR、VARCHAR、STRING -> PG_TEXT FLOAT、DOUBLE ->
     * PG_DOUBLE_PRECISION
     */
    public static class PgTypeNormalizationTransformer extends PgTypeTransformer {
        public PgTypeNormalizationTransformer(boolean isPrimaryKey) {
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

    /**
     * Transforms CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column}
     * in Normalization mode. TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT CHAR、VARCHAR、STRING ->
     * PG_TEXT FLOAT、DOUBLE -> PG_DOUBLE_PRECISION
     */
    public static class HoloColumnNormalizationTransformer extends HoloColumnTransformer {
        public HoloColumnNormalizationTransformer(boolean isPrimaryKey) {
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

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position and coverter to data formant which hologres record need.
     *
     * @param fieldType the element type of the RecordData
     * @param fieldPos the element position of the RecordData
     */
    public static RecordData.FieldGetter createDataTypeToRecordFieldGetter(
            DataType fieldType, int fieldPos, boolean isPrimaryKey) {
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
