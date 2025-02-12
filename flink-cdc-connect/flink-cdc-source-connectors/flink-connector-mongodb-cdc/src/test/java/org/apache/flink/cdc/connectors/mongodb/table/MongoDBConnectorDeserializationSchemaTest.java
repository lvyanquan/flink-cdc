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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link
 * org.apache.flink.cdc.connectors.mongodb.table.MongoDBConnectorDeserializationSchema}.
 */
public class MongoDBConnectorDeserializationSchemaTest {

    @ParameterizedTest
    @MethodSource({
        "testConvertToBooleanProvider",
        "testConvertToTinyIntProvider",
        "testConvertToSmallIntProvider",
        "testConvertToIntProvider",
        "testConvertToBigIntProvider",
        "testConvertToDoubleProvider",
        "testConvertToFloatProvider",
        "testConvertToDateProvider",
        "testConvertToTimeProvider",
        "testConvertToTimestampProvider",
        "testConvertToBinaryProvider",
        "testConvertToStringProvider",
    })
    public void testConvertDocument(
            LogicalType logicalType,
            BsonDocument document,
            Optional<Class<Throwable>> exception,
            String expected)
            throws Exception {
        RowType rowType =
                new RowType(
                        true, Collections.singletonList(new RowType.RowField("a", logicalType)));
        MongoDBConnectorDeserializationSchema deserializationSchema =
                new MongoDBConnectorDeserializationSchema(
                        rowType,
                        new MetadataConverter[] {},
                        TypeInformation.of(RowData.class),
                        ZoneId.systemDefault(),
                        false);

        if (!exception.isPresent()) {
            GenericRowData rowData = deserializationSchema.extractRowData(document);
            assertEquals(expected, rowData.toString());
        } else {
            Throwable actualException =
                    assertThrows(
                            exception.get(), () -> deserializationSchema.extractRowData(document));
            assertEquals(expected, actualException.getMessage());
        }
    }

    static Stream<Arguments> testConvertToBooleanProvider() {
        LogicalType logicalType = new BooleanType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument document4 = new BsonDocument("a", new BsonInt64(2));
        BsonDocument document5 = new BsonDocument("a", new BsonString("true"));
        BsonDocument document6 = new BsonDocument("a", new BsonString("false"));
        BsonDocument document7 = new BsonDocument("a", new BsonTimestamp());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(true)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(true)"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(false)"),
                Arguments.of(logicalType, document5, Optional.empty(), "+I(true)"),
                Arguments.of(logicalType, document6, Optional.empty(), "+I(false)"),
                Arguments.of(
                        logicalType,
                        document7,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to boolean from unexpected value '%s' of type %s",
                                document7.get("a"), document7.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToTinyIntProvider() {
        LogicalType logicalType = new TinyIntType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument document4 = new BsonDocument("a", new BsonInt64(2));
        BsonDocument document5 = new BsonDocument("a", new BsonString("10"));
        BsonDocument document6 = new BsonDocument("a", new BsonTimestamp());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(1)"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(2)"),
                Arguments.of(logicalType, document5, Optional.empty(), "+I(10)"),
                Arguments.of(
                        logicalType,
                        document6,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to tinyint from unexpected value '%s' of type %s",
                                document6.get("a"), document6.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToSmallIntProvider() {
        LogicalType logicalType = new SmallIntType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument document4 = new BsonDocument("a", new BsonInt64(2));
        BsonDocument document5 = new BsonDocument("a", new BsonString("10"));
        BsonDocument document6 = new BsonDocument("a", new BsonTimestamp());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(1)"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(2)"),
                Arguments.of(logicalType, document5, Optional.empty(), "+I(10)"),
                Arguments.of(
                        logicalType,
                        document6,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to smallint from unexpected value '%s' of type %s",
                                document6.get("a"), document6.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToIntProvider() {
        LogicalType logicalType = new IntType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(Integer.MAX_VALUE));
        BsonDocument document4 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_ZERO));
        BsonDocument document5 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.NEGATIVE_INFINITY));
        BsonDocument document6 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
        BsonDocument document7 = new BsonDocument("a", new BsonString("10"));
        BsonDocument document8 = new BsonDocument("a", new BsonDateTime(2000));
        BsonDocument document9 = new BsonDocument("a", new BsonTimestamp(1, 0));
        BsonDocument document10 = new BsonDocument("a", new BsonArray());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1)"),
                Arguments.of(
                        logicalType,
                        document3,
                        Optional.empty(),
                        String.format("+I(%d)", Integer.MAX_VALUE)),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(0)"),
                Arguments.of(
                        logicalType,
                        document5,
                        Optional.empty(),
                        String.format("+I(%d)", Integer.MIN_VALUE)),
                Arguments.of(
                        logicalType,
                        document6,
                        Optional.empty(),
                        String.format("+I(%d)", Integer.MAX_VALUE)),
                Arguments.of(logicalType, document7, Optional.empty(), "+I(10)"),
                Arguments.of(logicalType, document8, Optional.empty(), "+I(2)"),
                Arguments.of(logicalType, document9, Optional.empty(), "+I(1)"),
                Arguments.of(
                        logicalType,
                        document10,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to integer from unexpected value '%s' of type %s",
                                document10.get("a"), document10.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToBigIntProvider() {
        LogicalType logicalType = new BigIntType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt64(Long.MAX_VALUE));
        BsonDocument document4 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_ZERO));
        BsonDocument document5 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.NEGATIVE_INFINITY));
        BsonDocument document6 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
        BsonDocument document7 = new BsonDocument("a", new BsonString("10"));
        BsonDocument document8 = new BsonDocument("a", new BsonDateTime(2000));
        BsonDocument document9 = new BsonDocument("a", new BsonTimestamp(1, 0));
        BsonDocument document10 = new BsonDocument("a", new BsonArray());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1)"),
                Arguments.of(
                        logicalType,
                        document3,
                        Optional.empty(),
                        String.format("+I(%d)", Long.MAX_VALUE)),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(0)"),
                Arguments.of(
                        logicalType,
                        document5,
                        Optional.empty(),
                        String.format("+I(%d)", Long.MIN_VALUE)),
                Arguments.of(
                        logicalType,
                        document6,
                        Optional.empty(),
                        String.format("+I(%d)", Long.MAX_VALUE)),
                Arguments.of(logicalType, document7, Optional.empty(), "+I(10)"),
                Arguments.of(logicalType, document8, Optional.empty(), "+I(2000)"),
                Arguments.of(logicalType, document9, Optional.empty(), "+I(1000)"),
                Arguments.of(
                        logicalType,
                        document10,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to long from unexpected value '%s' of type %s",
                                document10.get("a"), document10.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToDoubleProvider() {
        LogicalType logicalType = new DoubleType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt64(Long.MAX_VALUE));
        BsonDocument document4 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_ZERO));
        BsonDocument document5 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.NEGATIVE_INFINITY));
        BsonDocument document6 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
        BsonDocument document7 = new BsonDocument("a", new BsonString("10.1"));
        BsonDocument document8 = new BsonDocument("a", new BsonTimestamp());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1.0)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(9.223372036854776E18)"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(0.0)"),
                Arguments.of(
                        logicalType, document5, Optional.empty(), "+I(-1.7976931348623157E308)"),
                Arguments.of(
                        logicalType, document6, Optional.empty(), "+I(1.7976931348623157E308)"),
                Arguments.of(logicalType, document7, Optional.empty(), "+I(10.1)"),
                Arguments.of(
                        logicalType,
                        document8,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to double from unexpected value '%s' of type %s",
                                document8.get("a"), document8.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToFloatProvider() {
        LogicalType logicalType = new FloatType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(Integer.MAX_VALUE));
        BsonDocument document4 = new BsonDocument("a", new BsonDouble(9.9));
        BsonDocument document5 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_ZERO));
        BsonDocument document6 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.NEGATIVE_INFINITY));
        BsonDocument document7 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_INFINITY));
        BsonDocument document8 = new BsonDocument("a", new BsonString("10.1"));
        BsonDocument document9 = new BsonDocument("a", new BsonTimestamp());
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1.0)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(2.14748365E9)"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(9.9)"),
                Arguments.of(logicalType, document5, Optional.empty(), "+I(0.0)"),
                Arguments.of(logicalType, document6, Optional.empty(), "+I(-3.4028235E38)"),
                Arguments.of(logicalType, document7, Optional.empty(), "+I(3.4028235E38)"),
                Arguments.of(logicalType, document8, Optional.empty(), "+I(10.1)"),
                Arguments.of(
                        logicalType,
                        document9,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to float from unexpected value '%s' of type %s",
                                document9.get("a"), document9.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToDateProvider() {
        LogicalType logicalType = new DateType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 =
                new BsonDocument("a", new BsonDateTime(Duration.ofDays(2).toMillis()));
        BsonDocument document3 =
                new BsonDocument(
                        "a", new BsonTimestamp((int) Duration.ofDays(3).toMillis() / 1000, 0));
        BsonDocument document4 = new BsonDocument("a", new BsonInt32(0));
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(2)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(3)"),
                Arguments.of(
                        logicalType,
                        document4,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to date from unexpected value '%s' of type %s",
                                document4.get("a"), document4.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToTimeProvider() {
        LogicalType logicalType = new TimeType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonDateTime(1000));
        BsonDocument document3 = new BsonDocument("a", new BsonTimestamp(2, 0));
        BsonDocument document4 = new BsonDocument("a", new BsonInt32(0));
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(28801000)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(28802000)"),
                Arguments.of(
                        logicalType,
                        document4,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to time from unexpected value '%s' of type %s",
                                document4.get("a"), document4.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToTimestampProvider() {
        LogicalType logicalType = new TimestampType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonDateTime(1000));
        BsonDocument document3 = new BsonDocument("a", new BsonTimestamp(2, 0));
        BsonDocument document4 = new BsonDocument("a", new BsonInt32(0));
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(1970-01-01T08:00:01)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I(1970-01-01T08:00:02)"),
                Arguments.of(
                        logicalType,
                        document4,
                        Optional.of(IllegalArgumentException.class),
                        String.format(
                                "Unable to convert to timestamp from unexpected value '%s' of type %s",
                                document4.get("a"), document4.get("a").getBsonType())));
    }

    static Stream<Arguments> testConvertToBinaryProvider() {
        LogicalType logicalType = new BinaryType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonBinary(new byte[] {1}));
        BsonDocument document3 = new BsonDocument("a", new BsonInt32(0));
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I([1])"),
                Arguments.of(
                        logicalType,
                        document3,
                        Optional.of(UnsupportedOperationException.class),
                        String.format(
                                "Unsupported BYTES value type: %s",
                                document3.get("a").getClass().getSimpleName())));
    }

    static Stream<Arguments> testConvertToStringProvider() {
        LogicalType logicalType = new VarCharType();
        BsonDocument document1 = new BsonDocument("a", new BsonNull());
        BsonDocument document2 = new BsonDocument("a", new BsonString("string"));
        BsonDocument document3 =
                new BsonDocument("a", new BsonDocument("key", new BsonString("value")));
        BsonDocument document4 = new BsonDocument("a", new BsonBinary(new byte[] {1}));
        BsonDocument document5 = new BsonDocument("a", new BsonBinary(UUID.randomUUID()));
        BsonDocument document6 =
                new BsonDocument("a", new BsonObjectId(new ObjectId("100000000000000000000101")));
        BsonDocument document7 = new BsonDocument("a", new BsonInt32(Integer.MAX_VALUE));
        BsonDocument document8 = new BsonDocument("a", new BsonInt64(Long.MAX_VALUE));
        BsonDocument document9 = new BsonDocument("a", new BsonDouble(Double.MAX_VALUE));
        BsonDocument document10 =
                new BsonDocument("a", new BsonDecimal128(Decimal128.POSITIVE_ZERO));
        BsonDocument document11 = new BsonDocument("a", new BsonBoolean(true));
        BsonDocument document12 =
                new BsonDocument(
                        "a",
                        new BsonArray(Arrays.asList(new BsonString("a"), new BsonString("b"))));
        BsonDocument document13 =
                new BsonDocument(
                        "a",
                        new BsonJavaScriptWithScope(
                                "code", new BsonDocument("key", new BsonString("value"))));
        BsonDocument document14 = new BsonDocument("a", new BsonSymbol("symbol"));
        BsonDocument document15 =
                new BsonDocument(
                        "a",
                        new BsonDbPointer("namespace", new ObjectId("100000000000000000000101")));
        return Stream.of(
                Arguments.of(logicalType, document1, Optional.empty(), "+I(null)"),
                Arguments.of(logicalType, document2, Optional.empty(), "+I(string)"),
                Arguments.of(logicalType, document3, Optional.empty(), "+I({\"key\": \"value\"})"),
                Arguments.of(logicalType, document4, Optional.empty(), "+I(01)"),
                Arguments.of(
                        logicalType,
                        document5,
                        Optional.empty(),
                        String.format("+I(%s)", document5.get("a").asBinary().asUuid().toString())),
                Arguments.of(
                        logicalType, document6, Optional.empty(), "+I(100000000000000000000101)"),
                Arguments.of(logicalType, document7, Optional.empty(), "+I(2147483647)"),
                Arguments.of(logicalType, document8, Optional.empty(), "+I(9223372036854775807)"),
                Arguments.of(
                        logicalType, document9, Optional.empty(), "+I(1.7976931348623157E308)"),
                Arguments.of(logicalType, document10, Optional.empty(), "+I(0)"),
                Arguments.of(logicalType, document11, Optional.empty(), "+I(true)"),
                Arguments.of(logicalType, document12, Optional.empty(), "+I([\"a\", \"b\"])"),
                Arguments.of(logicalType, document13, Optional.empty(), "+I(code)"),
                Arguments.of(logicalType, document14, Optional.empty(), "+I(symbol)"),
                Arguments.of(
                        logicalType, document15, Optional.empty(), "+I(100000000000000000000101)"));
    }

    @Test
    public void test() {
        BsonTimestamp timestamp = new BsonTimestamp(1, 0);
        System.out.println(timestamp.asTimestamp().getTime());
    }
}
