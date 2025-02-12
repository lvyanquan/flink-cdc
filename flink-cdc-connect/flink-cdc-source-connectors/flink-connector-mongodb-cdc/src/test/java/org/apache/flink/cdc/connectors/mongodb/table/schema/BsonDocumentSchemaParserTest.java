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

package org.apache.flink.cdc.connectors.mongodb.table.schema;

import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link org.apache.flink.cdc.connectors.mongodb.table.schema.BsonDocumentSchemaParser}.
 */
public class BsonDocumentSchemaParserTest {

    private static final BsonDocument BSON_DOCUMENT_WITH_ALL_TYPES;

    static {
        BSON_DOCUMENT_WITH_ALL_TYPES = new BsonDocument();
        BSON_DOCUMENT_WITH_ALL_TYPES.append("boolean", new BsonBoolean(true));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("int32", new BsonInt32(Integer.MAX_VALUE));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("int64", new BsonInt64(Long.MAX_VALUE));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("binary", new BsonBinary(new byte[] {Byte.MAX_VALUE}));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("double", new BsonDouble(Double.MAX_VALUE));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("string", new BsonString(""));
        BSON_DOCUMENT_WITH_ALL_TYPES.append(
                "decimal128", new BsonDecimal128(new Decimal128(BigDecimal.ONE)));
        BSON_DOCUMENT_WITH_ALL_TYPES.append(
                "object_id", new BsonObjectId(new ObjectId(new Date())));
        BSON_DOCUMENT_WITH_ALL_TYPES.append(
                "date_time", new BsonDateTime(System.currentTimeMillis()));
        BSON_DOCUMENT_WITH_ALL_TYPES.append(
                "timestamp", new BsonTimestamp(System.currentTimeMillis() / 1000));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("null", new BsonNull());
        BSON_DOCUMENT_WITH_ALL_TYPES.append("undefined", new BsonUndefined());
        BSON_DOCUMENT_WITH_ALL_TYPES.append("array", new BsonArray());
        BSON_DOCUMENT_WITH_ALL_TYPES.append(
                "document", new BsonDocument("key", new BsonString("value")));
        BSON_DOCUMENT_WITH_ALL_TYPES.append("javascript", new BsonJavaScript(""));
    }

    @Test
    public void testParseBsonTypes() {
        runSchemaParserTest(
                BSON_DOCUMENT_WITH_ALL_TYPES,
                SchemaSpec.newBuilder().build(),
                getSchema(
                        "ROW<`boolean` BOOLEAN, "
                                + "`int32` INT, "
                                + "`int64` BIGINT, "
                                + "`binary` BYTES, "
                                + "`double` DOUBLE, "
                                + "`string` STRING, "
                                + "`decimal128` DECIMAL(10, 0), "
                                + "`object_id` STRING, "
                                + "`date_time` TIMESTAMP_LTZ(3), "
                                + "`timestamp` TIMESTAMP_LTZ(0), "
                                + "`array` STRING, "
                                + "`document` STRING, "
                                + "`javascript` STRING>"),
                false,
                false);
    }

    @Test
    public void testParseNestedColumns() {
        BsonDocument document = new BsonDocument();
        document.append("unnested", new BsonString("unnested"));

        document.append("nested1", new BsonDocument("col1", new BsonInt32(1)));

        BsonDocument inner = new BsonDocument();
        inner.append("col1", new BsonString("col1"));
        inner.append("col2", new BsonDocument("col2_1", new BsonString("col2_1")));
        document.append("nested2", inner);

        runSchemaParserTest(
                document,
                SchemaSpec.newBuilder().build(),
                getSchema(
                        "ROW<`unnested` STRING, "
                                + "`nested1.col1` INT, "
                                + "`nested2.col1` STRING, "
                                + "`nested2.col2.col2_1` STRING>"),
                true,
                false);

        runSchemaParserTest(
                document,
                SchemaSpec.newBuilder().build(),
                getSchema("ROW<`unnested` STRING, " + "`nested1` STRING, " + "`nested2` STRING>"),
                false,
                false);
    }

    @Test
    public void testParsePrimitiveAsString() {
        runSchemaParserTest(
                BSON_DOCUMENT_WITH_ALL_TYPES,
                SchemaSpec.newBuilder().build(),
                getSchema(
                        "ROW<`boolean` STRING, "
                                + "`int32` STRING, "
                                + "`int64` STRING, "
                                + "`binary` STRING, "
                                + "`double` STRING, "
                                + "`string` STRING, "
                                + "`decimal128` STRING, "
                                + "`object_id` STRING, "
                                + "`date_time` STRING, "
                                + "`timestamp` STRING, "
                                + "`array` STRING, "
                                + "`document` STRING, "
                                + "`javascript` STRING>"),
                false,
                true);
    }

    @Test
    public void testParseWithBaseSchema() {
        BsonDocument document = new BsonDocument();
        document.append("col1", new BsonInt32(1));
        document.append("col2", new BsonInt32(1));
        document.append("col3", new BsonInt64(1));
        document.append("col4", new BsonString(""));
        document.append("col5", new BsonDateTime(System.currentTimeMillis()));
        document.append("col6", new BsonString(""));
        document.append("col7", new BsonString(""));

        runSchemaParserTest(
                document,
                getSchema(
                        "ROW<`col1` INT, "
                                + "`col2` BIGINT, "
                                + "`col3` INT, "
                                + "`col4` INT, "
                                + "`col5` INT>"),
                getSchema(
                        "ROW<`col1` INT, "
                                + "`col2` BIGINT, "
                                + "`col3` BIGINT, "
                                + "`col4` STRING, "
                                + "`col5` STRING, "
                                + "`col6` STRING, "
                                + "`col7` STRING>"),
                false,
                false);
    }

    @Test
    public void testParseSchemaWithPrimaryKey() {
        BsonDocument document = new BsonDocument();
        document.append(MongoDBEnvelope.ID_FIELD, new BsonObjectId(new ObjectId(new Date())));
        document.append("col1", new BsonInt32(0));
        document.append("col2", new BsonArray());
        document.append("col3", new BsonDocument("key", new BsonString("value")));

        runSchemaParserTest(
                document,
                SchemaSpec.newBuilder().build(),
                getSchema(
                        "ROW<`_id` STRING NOT NULL, "
                                + "`col1` INT, "
                                + "`col2` STRING, "
                                + "`col3` STRING>"),
                false,
                false);
    }

    private void runSchemaParserTest(
            BsonDocument bsonDocument,
            SchemaSpec baseSchema,
            SchemaSpec expectedSchema,
            boolean flattenNestedColumns,
            boolean primitiveAsString) {

        BsonDocumentSchemaParser parser =
                new BsonDocumentSchemaParser(flattenNestedColumns, primitiveAsString);
        Optional<SchemaSpec> schemaOp = parser.parseDocumentSchema(bsonDocument, baseSchema);
        assertTrue(schemaOp.isPresent());
        assertEquals(expectedSchema, schemaOp.get());
    }

    private static SchemaSpec getSchema(String rowType) {
        return SchemaSpec.fromRowType((RowType) LogicalTypeParser.parse(rowType));
    }
}
