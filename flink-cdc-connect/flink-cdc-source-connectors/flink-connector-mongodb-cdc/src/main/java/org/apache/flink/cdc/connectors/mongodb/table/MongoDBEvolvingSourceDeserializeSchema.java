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
import org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoRecordUtils;
import org.apache.flink.cdc.connectors.mongodb.table.schema.BsonDocumentSchemaParser;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.inferIsRegularExpression;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoRecordUtils.extractBsonDocument;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.SchemaUtils.getMetadataConverters;

/**
 * The deserializer to deserialize the records from the Debezium to the records in the Flink. {@link
 * MongoDBEvolvingSourceDeserializeSchema} also has the ability to apply the schema change when some
 * schema changes happens.
 */
public class MongoDBEvolvingSourceDeserializeSchema
        implements DebeziumDeserializationSchema<SourceRecord> {
    private static final Logger LOG =
            LoggerFactory.getLogger(MongoDBEvolvingSourceDeserializeSchema.class);

    private static final TypeInformation<RowData> TYPE_INFO_PLACE_HOLDER =
            TypeInformation.of(RowData.class);

    private final Set<MongoDBTableSpec> capturedTables;
    private final TypeInformation<SourceRecord> resultTypeInfo;
    private final ZoneId localTimeZone;
    private final boolean flattenNestedColumns;
    private final boolean enableFullDocPrePostImage;
    private final SimpleCollector collector;
    private final Map<MongoDBTableSpec, DebeziumDeserializationSchema<RowData>> cachedDeserializer;
    private final Map<MongoDBTableSpec, SchemaSpec> cachedSchema;
    private final Map<TableId, List<MongoDBTableSpec>> cachedMongoTableSpecs;
    private final BsonDocumentSchemaParser bsonDocumentSchemaParser;

    public MongoDBEvolvingSourceDeserializeSchema(
            Set<MongoDBTableSpec> capturedTables,
            ZoneId localTimeZone,
            TypeInformation<SourceRecord> resultTypeInfo,
            boolean flattenNestedColumns,
            boolean primitiveAsString,
            boolean enableFullDocPrePostImage) {
        this.capturedTables = capturedTables;
        this.localTimeZone = localTimeZone;
        this.resultTypeInfo = resultTypeInfo;
        this.flattenNestedColumns = flattenNestedColumns;
        this.enableFullDocPrePostImage = enableFullDocPrePostImage;
        this.collector = new SimpleCollector();

        this.cachedDeserializer = new HashMap<>();
        this.cachedSchema = new HashMap<>();
        this.cachedMongoTableSpecs = new HashMap<>();
        this.bsonDocumentSchemaParser =
                new BsonDocumentSchemaParser(flattenNestedColumns, primitiveAsString);

        initializeDeserializer();
    }

    @Override
    public void deserialize(
            org.apache.kafka.connect.source.SourceRecord record, Collector<SourceRecord> out)
            throws Exception {
        TableId tableId = MongoRecordUtils.getTableId(record);
        List<MongoDBTableSpec> tableSpecs = getTableSpec(tableId);

        collector.innerCollector = out;
        for (MongoDBTableSpec tableSpec : tableSpecs) {
            Optional<BsonDocument> documentOp = getFullDocument(record);
            if (documentOp.isPresent()) {
                BsonDocument document = documentOp.get();
                SchemaSpec currentSchema = cachedSchema.get(tableSpec);
                Optional<SchemaSpec> schemaOp =
                        bsonDocumentSchemaParser.parseDocumentSchema(document, currentSchema);
                if (!schemaOp.isPresent()) {
                    LOG.error(
                            "Failed to parse schema for bson document: {}, ignore this record.",
                            document.toJson());
                    return;
                }
                SchemaSpec newSchema = schemaOp.get();
                if (!Objects.equals(currentSchema, newSchema)) {
                    registerOrUpdateSchema(tableSpec, newSchema);
                }
            }
            collector.tableSpec = tableSpec;
            cachedDeserializer.get(tableSpec).deserialize(record, collector);
        }
    }

    @Override
    public TypeInformation<SourceRecord> getProducedType() {
        return resultTypeInfo;
    }

    private List<MongoDBTableSpec> getTableSpec(TableId tableId) {
        if (!cachedMongoTableSpecs.containsKey(tableId)) {
            List<MongoDBTableSpec> tableSpecs =
                    capturedTables.stream()
                            .filter(
                                    tableSpec ->
                                            isTableIdMatch(
                                                    tableId,
                                                    tableSpec.getDatabase(),
                                                    tableSpec.getCollection()))
                            .collect(Collectors.toList());
            cachedMongoTableSpecs.put(tableId, tableSpecs);
        }
        return cachedMongoTableSpecs.get(tableId);
    }

    private boolean isTableIdMatch(
            TableId tableId, @Nullable String database, @Nullable String collection) {
        String databaseName = tableId.catalog();
        String collName = tableId.table();
        if (!StringUtils.isEmpty(database)) {
            if (!inferIsRegularExpression(database)) {
                if (!database.equals(databaseName)) {
                    return false;
                }
            } else {
                Pattern pattern = CollectionDiscoveryUtils.completionPattern(database);
                Matcher matcher = pattern.matcher(databaseName);
                if (!matcher.matches()) {
                    return false;
                }
            }
        }
        if (!StringUtils.isEmpty(collection)) {
            if (!inferIsRegularExpression(collection)) {
                return collection.equals(collName);
            } else {
                Pattern pattern = CollectionDiscoveryUtils.completionPattern(collection);
                Matcher matcher = pattern.matcher(databaseName + "." + collName);
                return matcher.matches();
            }
        }
        return true;
    }

    private void initializeDeserializer() {
        for (MongoDBTableSpec tableSpec : capturedTables) {
            DataType physicalDataType = tableSpec.getPhysicalDataType();
            RowType physicalRowType = (RowType) physicalDataType.getLogicalType();
            registerOrUpdateSchema(tableSpec, SchemaSpec.fromRowType(physicalRowType));
        }
    }

    private void registerOrUpdateSchema(MongoDBTableSpec tableSpec, SchemaSpec schema) {
        List<String> metadataKeys = tableSpec.getMetadataKeys();
        MetadataConverter[] metadataConverters = new MetadataConverter[0];
        if (metadataKeys != null && metadataKeys.size() > 0) {
            metadataConverters = getMetadataConverters(metadataKeys);
        }
        DebeziumDeserializationSchema<RowData> deserializationSchema =
                enableFullDocPrePostImage
                        ? new MongoDBConnectorFullChangelogDeserializationSchema(
                                schema.toRowType(),
                                metadataConverters,
                                TYPE_INFO_PLACE_HOLDER,
                                localTimeZone,
                                flattenNestedColumns)
                        : new MongoDBConnectorDeserializationSchema(
                                schema.toRowType(),
                                metadataConverters,
                                TYPE_INFO_PLACE_HOLDER,
                                localTimeZone,
                                flattenNestedColumns);
        cachedDeserializer.put(tableSpec, deserializationSchema);
        cachedSchema.put(tableSpec, schema);
    }

    private Optional<BsonDocument> getFullDocument(
            org.apache.kafka.connect.source.SourceRecord record) {
        Struct value = (Struct) record.value();
        BsonDocument fullDocument =
                extractBsonDocument(
                        value, record.valueSchema(), MongoDBEnvelope.FULL_DOCUMENT_FIELD);
        // for delete, update (not enable full doc), document is null
        return Optional.ofNullable(fullDocument);
    }

    private class SimpleCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private transient Collector<SourceRecord> innerCollector;
        private transient MongoDBTableSpec tableSpec;

        @Override
        public void collect(RowData record) {
            innerCollector.collect(
                    new SourceRecord(
                            tableSpec.getTablePathInFlink(), cachedSchema.get(tableSpec), record));
        }

        @Override
        public void close() {}
    }
}
