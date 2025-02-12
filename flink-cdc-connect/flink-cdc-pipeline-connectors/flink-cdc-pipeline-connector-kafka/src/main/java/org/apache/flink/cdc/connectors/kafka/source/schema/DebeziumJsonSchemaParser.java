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

package org.apache.flink.cdc.connectors.kafka.source.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.JsonToRowDataInSourceRecordConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Parse one kafka record with debezium json format to get {@link TableId} and {@link Schema}. */
public class DebeziumJsonSchemaParser implements RecordSchemaParser {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonSchemaParser.class);
    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");

    private final boolean schemaInclude;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;

    public DebeziumJsonSchemaParser(
            boolean schemaInclude,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        this.schemaInclude = schemaInclude;
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);

        final RowType jsonRowType = createJsonRowType(schemaInclude);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used
                        TypeInformation.of(RowData.class),
                        false,
                        false,
                        timestampFormat,
                        zoneId);
        this.jsonToSourceRecordConverter =
                new JsonToSourceRecordConverter(
                        // the object path is never used
                        OBJECT_PATH_PLACEHOLDER,
                        new RowType(Collections.emptyList()),
                        new JsonToRowDataInSourceRecordConverters(
                                false, false, timestampFormat, zoneId),
                        false,
                        false,
                        primitiveAsString);
    }

    @Override
    public void open() throws Exception {
        jsonDeserializer.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return SimpleUserCodeClassLoader.create(
                                DebeziumJsonSchemaParser.class.getClassLoader());
                    }
                });
    }

    @Override
    public Optional<Tuple2<TableId, Schema>> parseRecordSchema(
            ConsumerRecord<byte[], byte[]> record) throws IOException {
        byte[] message = record.value();
        if (message == null || message.length == 0) {
            return Optional.empty();
        }
        try {
            GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            GenericRowData payload = schemaInclude ? (GenericRowData) row.getField(0) : row;

            String database = readProperty(payload, 2, "db");
            String table = readProperty(payload, 2, "table");
            if (database == null || table == null) {
                return Optional.empty();
            }
            TableId tableId = TableId.tableId(database, table);

            StringData before = payload.getString(0);
            StringData after = payload.getString(1);
            List<SchemaSpec> schemaSpecs = new ArrayList<>();
            if (before != null) {
                schemaSpecs.add(getSourceRecordByJson(before.toString()).getSchema());
            }
            if (after != null) {
                schemaSpecs.add(getSourceRecordByJson(after.toString()).getSchema());
            }
            SchemaSpec finalSchema = SchemaUtils.mergeSchemaSpec(schemaSpecs);
            return Optional.of(Tuple2.of(tableId, SchemaUtils.convert(finalSchema)));
        } catch (Exception e) {
            LOG.debug("Failed to parse schema by kafka record.", e);
            return Optional.empty();
        }
    }

    private SourceRecord getSourceRecordByJson(String json) throws IOException {
        JsonParser root = objectMapper.getFactory().createParser(json);
        if (root.currentToken() == null) {
            root.nextToken();
        }
        return jsonToSourceRecordConverter.convert(root);
    }

    private static RowType createJsonRowType(boolean schemaInclude) {
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("before", DataTypes.STRING()),
                        DataTypes.FIELD("after", DataTypes.STRING()),
                        DataTypes.FIELD(
                                "source",
                                DataTypes.MAP(
                                                DataTypes.STRING().nullable(),
                                                DataTypes.STRING().nullable())
                                        .nullable()));
        DataType root = payload;

        if (schemaInclude) {
            root = DataTypes.ROW(DataTypes.FIELD("payload", payload));
        }
        return (RowType) root.getLogicalType();
    }

    private static String readProperty(GenericRowData row, int pos, String key) {
        GenericMapData map = (GenericMapData) row.getMap(pos);
        if (map == null) {
            return null;
        }
        StringData value = (StringData) map.get(StringData.fromString(key));
        return value == null ? null : value.toString();
    }
}
