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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/** Parse one kafka record with canal json format to get {@link TableId} and {@link Schema}. */
public class CanalJsonSchemaParser implements RecordSchemaParser {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CanalJsonSchemaParser.class);

    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");
    private static final String DATABASE_KEY = "database";
    private static final String TABLE_KEY = "table";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Pattern databasePattern;
    private final Pattern tablePattern;
    private final JsonRowDataDeserializationSchema jsonDeserializer;
    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;

    public CanalJsonSchemaParser(
            @Nullable String database,
            @Nullable String table,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        this.databasePattern = database == null ? null : Pattern.compile(database);
        this.tablePattern = table == null ? null : Pattern.compile(table);

        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);

        final RowType jsonRowType = createJsonRowType();
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
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
    public Optional<Tuple2<TableId, Schema>> parseRecordKeySchema(
            ConsumerRecord<byte[], byte[]> record) {
        return parseSchema(record.key());
    }

    @Override
    public Optional<Tuple2<TableId, Schema>> parseRecordValueSchema(
            ConsumerRecord<byte[], byte[]> record) {
        return parseSchema(record.value());
    }

    private Optional<Tuple2<TableId, Schema>> parseSchema(byte[] message) {
        if (message == null || message.length == 0) {
            return Optional.empty();
        }
        try {
            final JsonNode root = jsonDeserializer.deserializeToJsonNode(message);
            JsonNode databaseNode = root.get(DATABASE_KEY);
            JsonNode tableNode = root.get(TABLE_KEY);
            if (databaseNode == null || tableNode == null) {
                return Optional.empty();
            }
            String database = databaseNode.asText();
            String table = tableNode.asText();
            if (databasePattern != null && !databasePattern.matcher(database).matches()) {
                return Optional.empty();
            }
            if (tablePattern != null && !tablePattern.matcher(table).matches()) {
                return Optional.empty();
            }
            TableId tableId = TableId.tableId(database, table);

            final GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
            List<String> dataList = getStringArray(row, 0);
            List<String> oldList = getStringArray(row, 1);
            List<SchemaSpec> schemaSpecs = new ArrayList<>();
            for (String json : dataList) {
                SourceRecord sourceRecord = getSourceRecordByJson(json);
                schemaSpecs.add(sourceRecord.getSchema());
            }
            for (String json : oldList) {
                SourceRecord sourceRecord = getSourceRecordByJson(json);
                schemaSpecs.add(sourceRecord.getSchema());
            }
            if (schemaSpecs.isEmpty()) {
                return Optional.empty();
            }
            SchemaSpec finalSchema = SchemaUtils.mergeSchemaSpec(schemaSpecs);
            List<String> pkNames = getStringArray(row, 2);
            Schema schema = SchemaUtils.convert(finalSchema, pkNames);
            return Optional.of(Tuple2.of(tableId, schema));
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

    private static List<String> getStringArray(GenericRowData row, int pos) {
        ArrayData arrayData = row.getArray(pos);
        if (arrayData == null) {
            return Collections.emptyList();
        }
        List<String> stringList = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); i++) {
            stringList.add(arrayData.getString(i).toString());
        }
        return stringList;
    }

    private static RowType createJsonRowType() {
        DataType root =
                DataTypes.ROW(
                        DataTypes.FIELD("data", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("old", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("pkNames", DataTypes.ARRAY(DataTypes.STRING())));
        return (RowType) root.getLogicalType();
    }
}
