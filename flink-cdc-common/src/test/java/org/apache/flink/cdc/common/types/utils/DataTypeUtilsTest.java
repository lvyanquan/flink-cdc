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

package org.apache.flink.cdc.common.types.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link org.apache.flink.cdc.common.types.utils.DataTypeUtils}. */
class DataTypeUtilsTest {
    private static final int DEFAULT_PRECISION = 6;
    private static final int DEFAULT_SCALE = 3;
    private static final int DEFAULT_LEN = 10;

    private static final DataType[] ALL_TYPES =
            new DataType[] {
                DataTypes.BOOLEAN(),
                DataTypes.BYTES(),
                DataTypes.BINARY(10),
                DataTypes.VARBINARY(10),
                DataTypes.CHAR(10),
                DataTypes.VARCHAR(10),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.BIGINT(),
                DataTypes.DOUBLE(),
                DataTypes.FLOAT(),
                DataTypes.DECIMAL(6, 3),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIME(6),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP(6),
                DataTypes.TIMESTAMP_LTZ(),
                DataTypes.TIMESTAMP_LTZ(6),
                DataTypes.TIMESTAMP_TZ(),
                DataTypes.TIMESTAMP_TZ(6),
                DataTypes.ARRAY(DataTypes.BIGINT()),
                DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.STRING()),
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.STRING(), "desc")),
                DataTypes.ROW(DataTypes.SMALLINT(), DataTypes.STRING())
            };

    private static final org.apache.flink.table.types.DataType[] FLINK_TYPES =
            new org.apache.flink.table.types.DataType[] {
                BOOLEAN(),
                BYTES(),
                BINARY(10),
                VARBINARY(10),
                CHAR(10),
                VARCHAR(10),
                STRING(),
                INT(),
                TINYINT(),
                SMALLINT(),
                BIGINT(),
                DOUBLE(),
                FLOAT(),
                DECIMAL(6, 3),
                DATE(),
                TIME(),
                TIME(6),
                TIMESTAMP(),
                TIMESTAMP(6),
                TIMESTAMP_LTZ(),
                TIMESTAMP_LTZ(6),
                TIMESTAMP_WITH_TIME_ZONE(),
                TIMESTAMP_WITH_TIME_ZONE(6),
                ARRAY(BIGINT()),
                MAP(SMALLINT(), STRING()),
                ROW(FIELD("f1", STRING()), FIELD("f2", STRING(), "desc")),
                ROW(SMALLINT(), STRING())
            };

    @Test
    void testToFlinkDataType() {
        DataType cdcNullableDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i]))
                                .collect(Collectors.toList()));
        DataType cdcNotNullDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        org.apache.flink.table.types.DataType nullableDataType =
                DataTypeUtils.toFlinkDataType(cdcNullableDataType);
        org.apache.flink.table.types.DataType notNullDataType =
                DataTypeUtils.toFlinkDataType(cdcNotNullDataType);

        org.apache.flink.table.types.DataType expectedNullableDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i]))
                                .collect(Collectors.toList()));
        org.apache.flink.table.types.DataType expectedNotNullDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        assertThat(nullableDataType).isEqualTo(expectedNullableDataType);
        assertThat(notNullDataType).isEqualTo(expectedNotNullDataType);
    }

    @Test
    public void testFromFlinkType() {
        org.apache.flink.table.types.DataType flinkNullableDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i]))
                                .collect(Collectors.toList()));

        org.apache.flink.table.types.DataType flinkNotNullDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        DataType nullableDataType = DataTypeUtils.fromFlinkDataType(flinkNullableDataType);
        DataType notNullDataType = DataTypeUtils.fromFlinkDataType(flinkNotNullDataType);

        DataType expectedNullableDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i]))
                                .collect(Collectors.toList()));
        DataType expectedNotNullDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        assertThat(nullableDataType).isEqualTo(expectedNullableDataType);
        assertThat(notNullDataType).isEqualTo(expectedNotNullDataType);
    }

    private static Stream<Arguments> parameterProvider() {
        List<Arguments> testCases = new ArrayList<>();
        // Same type
        Arrays.stream(ALL_TYPES)
                .forEach(dataType -> testCases.add(Arguments.of(dataType, dataType, dataType)));
        // Different type whose common parent type is string
        for (DataType left : ALL_TYPES) {
            for (DataType right : ALL_TYPES) {
                if (!isCompatibleType(left, right)) {
                    testCases.add(Arguments.of(left, right, DataTypes.STRING()));
                }
            }
        }
        testCases.addAll(
                Arrays.asList(
                        // Same type with different length
                        Arguments.of(
                                DataTypes.CHAR(DEFAULT_LEN),
                                DataTypes.CHAR(DEFAULT_LEN - 1),
                                DataTypes.VARCHAR(DEFAULT_LEN)),
                        Arguments.of(
                                DataTypes.VARCHAR(DEFAULT_LEN),
                                DataTypes.VARCHAR(DEFAULT_LEN - 1),
                                DataTypes.VARCHAR(DEFAULT_LEN)),
                        Arguments.of(
                                DataTypes.VARBINARY(DEFAULT_LEN),
                                DataTypes.VARBINARY(DEFAULT_LEN - 1),
                                DataTypes.VARBINARY(DEFAULT_LEN)),
                        // Same type with different precision
                        Arguments.of(
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE),
                                DataTypes.DECIMAL(DEFAULT_PRECISION - 1, DEFAULT_SCALE - 1),
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE)),
                        Arguments.of(
                                DataTypes.TIME(DEFAULT_PRECISION),
                                DataTypes.TIME(DEFAULT_PRECISION - 1),
                                DataTypes.TIME(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.TIMESTAMP(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP(DEFAULT_PRECISION - 1),
                                DataTypes.TIMESTAMP(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION - 1),
                                DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.TIMESTAMP_TZ(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP_TZ(DEFAULT_PRECISION - 1),
                                DataTypes.TIMESTAMP_TZ(DEFAULT_PRECISION)),
                        // Different type whose common parent type is not varchar
                        Arguments.of(
                                DataTypes.BINARY(DEFAULT_LEN),
                                DataTypes.VARBINARY(DEFAULT_LEN - 1),
                                DataTypes.VARBINARY(DEFAULT_LEN)),
                        Arguments.of(
                                DataTypes.CHAR(DEFAULT_LEN),
                                DataTypes.VARCHAR(DEFAULT_LEN - 1),
                                DataTypes.VARCHAR(DEFAULT_LEN)),
                        Arguments.of(
                                DataTypes.DATE(),
                                DataTypes.TIMESTAMP(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.DATE(),
                                DataTypes.TIMESTAMP_TZ(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP_TZ(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.DATE(),
                                DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION),
                                DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION)),
                        Arguments.of(
                                DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.SMALLINT()),
                        Arguments.of(DataTypes.TINYINT(), DataTypes.INT(), DataTypes.INT()),
                        Arguments.of(DataTypes.TINYINT(), DataTypes.BIGINT(), DataTypes.BIGINT()),
                        Arguments.of(
                                DataTypes.TINYINT(),
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE),
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE)),
                        Arguments.of(DataTypes.TINYINT(), DataTypes.FLOAT(), DataTypes.FLOAT()),
                        Arguments.of(DataTypes.TINYINT(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                        Arguments.of(DataTypes.INT(), DataTypes.FLOAT(), DataTypes.FLOAT()),
                        Arguments.of(DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                        Arguments.of(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                        Arguments.of(
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE),
                                DataTypes.FLOAT(),
                                DataTypes.DOUBLE()),
                        Arguments.of(
                                DataTypes.DECIMAL(DEFAULT_PRECISION, DEFAULT_SCALE),
                                DataTypes.DOUBLE(),
                                DataTypes.DOUBLE())));

        return testCases.stream();
    }

    @ParameterizedTest(name = "{index}: merge {0} and {1}, expect {2}")
    @MethodSource("parameterProvider")
    public void testFindCommonType(
            DataType dataType0, DataType dataType1, DataType expectedMergeType) {
        assertThat(DataTypeUtils.findCommonType(Arrays.asList(dataType0, dataType1)))
                .isEqualTo(expectedMergeType);
    }

    private static boolean isCompatibleType(DataType dataType0, DataType dataType1) {
        if (dataType0.getTypeRoot() == dataType1.getTypeRoot()) {
            return true;
        }

        if (dataType0.is(DataTypeFamily.NUMERIC) && dataType1.is(DataTypeFamily.NUMERIC)) {
            return true;
        }
        if (dataType0.is(DataTypeFamily.CHARACTER_STRING)
                && dataType1.is(DataTypeFamily.CHARACTER_STRING)) {
            return true;
        }
        if (dataType0.is(DataTypeFamily.BINARY_STRING)
                && dataType1.is(DataTypeFamily.BINARY_STRING)) {
            return true;
        }
        if ((dataType0.is(DataTypeFamily.TIMESTAMP) || dataType0.is(DataTypeRoot.DATE))
                && (dataType1.is(DataTypeFamily.TIMESTAMP) || dataType1.is(DataTypeRoot.DATE))) {
            return true;
        }
        return false;
    }
}
