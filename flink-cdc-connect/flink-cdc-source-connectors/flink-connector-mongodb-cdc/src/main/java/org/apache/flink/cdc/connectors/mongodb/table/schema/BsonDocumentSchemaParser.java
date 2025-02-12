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
import org.apache.flink.cdc.connectors.mongodb.source.utils.SchemaUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.types.DataType;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Get the schema from one bson document. */
public class BsonDocumentSchemaParser implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BsonDocumentSchemaParser.class);
    private static final Map<BsonType, DataType> BsonTypeToFlinkType = new HashMap<>();

    static {
        BsonTypeToFlinkType.put(BsonType.BOOLEAN, DataTypes.BOOLEAN());
        BsonTypeToFlinkType.put(BsonType.INT32, DataTypes.INT());
        BsonTypeToFlinkType.put(BsonType.INT64, DataTypes.BIGINT());
        BsonTypeToFlinkType.put(BsonType.BINARY, DataTypes.BYTES());
        BsonTypeToFlinkType.put(BsonType.DOUBLE, DataTypes.DOUBLE());
        BsonTypeToFlinkType.put(BsonType.STRING, DataTypes.STRING());
        BsonTypeToFlinkType.put(BsonType.DECIMAL128, DataTypes.DECIMAL(10, 0));
        BsonTypeToFlinkType.put(BsonType.OBJECT_ID, DataTypes.STRING());
        BsonTypeToFlinkType.put(BsonType.DATE_TIME, DataTypes.TIMESTAMP_LTZ(3));
        BsonTypeToFlinkType.put(BsonType.TIMESTAMP, DataTypes.TIMESTAMP_LTZ(0));
        BsonTypeToFlinkType.put(BsonType.NULL, DataTypes.NULL());
        BsonTypeToFlinkType.put(BsonType.UNDEFINED, DataTypes.NULL());
        BsonTypeToFlinkType.put(BsonType.ARRAY, DataTypes.STRING());
        BsonTypeToFlinkType.put(BsonType.DOCUMENT, DataTypes.STRING());
    }

    private final boolean flattenNestedColumns;
    private final boolean primitiveAsString;

    public BsonDocumentSchemaParser(boolean flattenNestedColumns, boolean primitiveAsString) {
        this.flattenNestedColumns = flattenNestedColumns;
        this.primitiveAsString = primitiveAsString;
    }

    /**
     * Parse the schema of a {@link BsonDocument}, based on the given schema. The returned schema
     * contains all columns in the base schema. If any columns' type is different from it in base
     * schema, it will find a common type as the final type.
     */
    public Optional<SchemaSpec> parseDocumentSchema(BsonDocument document, SchemaSpec baseSchema) {
        SchemaSpec.Builder builder = SchemaSpec.newBuilder();
        try {
            buildDocumentSchema(document, builder, "");
        } catch (Throwable t) {
            LOG.error("Failed to parse bson document.", t);
            return Optional.empty();
        }
        SchemaSpec currentSchema = builder.build();
        return Optional.of(SchemaUtils.mergeSchema(baseSchema, currentSchema));
    }

    private void buildDocumentSchema(
            BsonDocument document, SchemaSpec.Builder builder, String prefix) {
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            BsonValue value = entry.getValue();
            if (value.isNull() || value.getBsonType() == BsonType.UNDEFINED) {
                continue;
            }
            if (value.isDocument() && flattenNestedColumns) {
                buildDocumentSchema(
                        entry.getValue().asDocument(), builder, prefix + entry.getKey() + ".");
                continue;
            }
            DataType dataType;
            if (primitiveAsString) {
                dataType = DataTypes.STRING();
            } else {
                dataType =
                        BsonTypeToFlinkType.getOrDefault(value.getBsonType(), DataTypes.STRING());
            }
            String columnName = prefix + entry.getKey();
            if (MongoDBEnvelope.ID_FIELD.equals(columnName)) {
                builder.column(columnName, dataType.notNull());
            } else {
                builder.column(columnName, dataType);
            }
        }
    }
}
