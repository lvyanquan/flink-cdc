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

package org.apache.flink.cdc.connectors.kafka.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaMergingUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.source.KafkaDataSource;
import org.apache.flink.cdc.connectors.kafka.source.metadata.KafkaReadableMetadata;
import org.apache.flink.cdc.connectors.kafka.source.schema.SchemaAware;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A specific {@link KafkaRecordSchemaAwareDeserializationSchema} for {@link KafkaDataSource}. */
public class KafkaEventDeserializationSchema
        implements KafkaRecordSchemaAwareDeserializationSchema<Event> {

    @Nullable private final SchemaAwareDeserializationSchema<Event> keyDeserializationSchema;
    private final SchemaAwareDeserializationSchema<Event> valueDeserializationSchema;

    private final KeyCollector keyCollector;
    private final OutputCollector outputCollector;

    public KafkaEventDeserializationSchema(
            @Nullable SchemaAwareDeserializationSchema<Event> keyDeserializationSchema,
            SchemaAwareDeserializationSchema<Event> valueDeserializationSchema,
            @Nullable String keyPrefix,
            @Nullable String valuePrefix,
            List<KafkaReadableMetadata> readableMetadataList) {
        this.keyDeserializationSchema = keyDeserializationSchema;
        this.valueDeserializationSchema = valueDeserializationSchema;

        this.keyCollector = new KeyCollector();
        this.outputCollector =
                new OutputCollector(
                        keyDeserializationSchema != null,
                        valueDeserializationSchema,
                        keyPrefix,
                        valuePrefix,
                        readableMetadataList);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (keyDeserializationSchema != null) {
            keyDeserializationSchema.open(context);
        }
        valueDeserializationSchema.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> collector)
            throws IOException {
        outputCollector.inputRecord = record;
        outputCollector.collector = collector;

        // Default table id is topic name, it can be self-defined in deserializationSchema.
        TableId tableId = TableId.tableId(record.topic());
        if (keyDeserializationSchema != null) {
            keyDeserializationSchema.deserialize(record.key(), tableId, keyCollector);
            outputCollector.keySchema =
                    keyDeserializationSchema.getTableSchema(keyCollector.tableId);
            outputCollector.keyRecordData = keyCollector.recordData;
        }
        valueDeserializationSchema.deserialize(record.value(), tableId, outputCollector);
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return new EventTypeInfo();
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        return valueDeserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setTableSchema(TableId tableId, Schema schema) {
        valueDeserializationSchema.setTableSchema(tableId, schema);
    }

    @Override
    public Schema getKeyTableSchema(TableId tableId) {
        if (keyDeserializationSchema == null) {
            return null;
        }
        return keyDeserializationSchema.getTableSchema(tableId);
    }

    @Override
    public void setKeyTableSchema(TableId tableId, Schema schema) {
        if (keyDeserializationSchema != null) {
            keyDeserializationSchema.setTableSchema(tableId, schema);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static final class KeyCollector implements Collector<Event>, Serializable {

        private static final long serialVersionUID = 1L;

        private TableId tableId;
        private RecordData recordData;

        @Override
        public void collect(Event event) {
            tableId = null;
            recordData = null;
            if (event == null) {
                return;
            }
            if (event instanceof CreateTableEvent) {
                tableId = ((CreateTableEvent) event).tableId();
                return;
            }
            if (!(event instanceof DataChangeEvent)) {
                throw new IllegalStateException(
                        "KeyDeserializationSchema should generate data change event, but event is: "
                                + event);
            }
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            if (dataChangeEvent.op() != OperationType.INSERT) {
                throw new IllegalStateException(
                        "KeyDeserializationSchema should generate data change event with type INSERT, but event is: "
                                + event);
            }
            tableId = dataChangeEvent.tableId();
            recordData = dataChangeEvent.after();
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    private static final class OutputCollector implements Collector<Event>, Serializable {

        private static final long serialVersionUID = 1L;

        private final boolean hasKeyFields;
        private final SchemaAware valueSchemas;
        @Nullable private final String keyPrefix;
        @Nullable private final String valuePrefix;
        private final List<KafkaReadableMetadata> readableMetadataList;

        private final Map<Schema, Schema> cachedKeySchemaWithPrefix;
        private transient Schema keySchema;
        private transient RecordData keyRecordData;
        private transient ConsumerRecord<?, ?> inputRecord;
        private transient Collector<Event> collector;

        OutputCollector(
                boolean hasKeyFields,
                SchemaAware valueSchemas,
                @Nullable String keyPrefix,
                @Nullable String valuePrefix,
                List<KafkaReadableMetadata> readableMetadataList) {
            this.hasKeyFields = hasKeyFields;
            this.valueSchemas = valueSchemas;
            this.keyPrefix = keyPrefix;
            this.valuePrefix = valuePrefix;
            this.readableMetadataList = readableMetadataList;
            this.cachedKeySchemaWithPrefix = new HashMap<>();
        }

        @Override
        public void collect(Event event) {
            if (event instanceof SchemaChangeEvent) {
                SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
                if (valuePrefix != null) {
                    schemaChangeEvent =
                            SchemaUtils.changeColumnName(
                                    schemaChangeEvent, col -> valuePrefix + col);
                }

                if (schemaChangeEvent instanceof CreateTableEvent && hasKeyFields) {
                    CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
                    Schema valueSchema = createTableEvent.getSchema();
                    Schema finalKeySchema = keySchema;
                    if (keyPrefix != null) {
                        finalKeySchema =
                                cachedKeySchemaWithPrefix.compute(
                                        keySchema,
                                        (k, v) ->
                                                v != null
                                                        ? v
                                                        : SchemaUtils.changeColumnName(
                                                                keySchema, col -> keyPrefix + col));
                    }

                    Schema joinedSchema =
                            SchemaMergingUtils.getJoinedColumnsSchema(finalKeySchema, valueSchema);
                    collector.collect(
                            new CreateTableEvent(createTableEvent.tableId(), joinedSchema));
                } else {
                    collector.collect(schemaChangeEvent);
                }
                return;
            }

            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            // Add key fields
            if (hasKeyFields) {
                Schema valueSchema = valueSchemas.getTableSchema(dataChangeEvent.tableId());
                if (dataChangeEvent.before() != null) {
                    RecordData joinedBefore =
                            combineKeyValueRecordData(
                                    keySchema,
                                    keyRecordData,
                                    valueSchema,
                                    dataChangeEvent.before());
                    dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, joinedBefore);
                }
                if (dataChangeEvent.after() != null) {
                    RecordData joinedAfter =
                            combineKeyValueRecordData(
                                    keySchema, keyRecordData, valueSchema, dataChangeEvent.after());
                    dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, joinedAfter);
                }
            }
            // Append metadata
            if (!readableMetadataList.isEmpty()) {
                Map<String, String> meta = new HashMap<>(dataChangeEvent.meta());
                readableMetadataList.forEach(
                        readableMetadata -> {
                            Object metadata = readableMetadata.getConverter().read(inputRecord);
                            meta.put(readableMetadata.getKey(), String.valueOf(metadata));
                        });
                collector.collect(DataChangeEvent.replaceMeta(dataChangeEvent, meta));
            } else {
                collector.collect(dataChangeEvent);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private RecordData combineKeyValueRecordData(
                Schema keySchema, RecordData keyData, Schema valueSchema, RecordData valueData) {
            int arity = keyData.getArity() + valueData.getArity();

            List<DataType> dataTypeList = keySchema.getColumnDataTypes();
            dataTypeList.addAll(valueSchema.getColumnDataTypes());
            DataType[] dataTypes = dataTypeList.toArray(new DataType[0]);

            List<RecordData.FieldGetter> keyFieldGetters =
                    SchemaUtils.createFieldGetters(keySchema);
            List<RecordData.FieldGetter> valueFieldGetters =
                    SchemaUtils.createFieldGetters(valueSchema);

            Object[] rowFields = new Object[arity];
            for (int i = 0; i < arity; i++) {
                if (i < keyData.getArity()) {
                    rowFields[i] = keyFieldGetters.get(i).getFieldOrNull(keyData);
                } else {
                    rowFields[i] =
                            valueFieldGetters.get(i - keyData.getArity()).getFieldOrNull(valueData);
                }
            }

            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(dataTypes);
            return generator.generate(rowFields);
        }
    }
}
