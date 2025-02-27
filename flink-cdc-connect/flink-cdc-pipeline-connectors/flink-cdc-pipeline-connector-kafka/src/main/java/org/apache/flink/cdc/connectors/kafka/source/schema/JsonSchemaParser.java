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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataInSourceRecordConverters;
import org.apache.flink.formats.json.JsonToSourceRecordConverter;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Optional;

/** Parse one kafka record with json format to get {@link Schema}. */
public class JsonSchemaParser implements RecordSchemaParser {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaParser.class);
    private static final ObjectPath OBJECT_PATH_PLACEHOLDER = new ObjectPath("db", "table");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonToSourceRecordConverter jsonToSourceRecordConverter;

    public JsonSchemaParser(
            boolean flattenNestedColumn,
            boolean primitiveAsString,
            TimestampFormat timestampFormat,
            ZoneId zoneId) {
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);

        this.jsonToSourceRecordConverter =
                new JsonToSourceRecordConverter(
                        OBJECT_PATH_PLACEHOLDER,
                        new RowType(Collections.emptyList()),
                        new JsonToRowDataInSourceRecordConverters(
                                false, false, timestampFormat, zoneId),
                        false,
                        flattenNestedColumn,
                        primitiveAsString);
    }

    @Override
    public void open() throws Exception {
        // nothing to do
    }

    @Override
    public Optional<Tuple2<TableId, Schema>> parseRecordValueSchema(
            ConsumerRecord<byte[], byte[]> record) {
        TableId tableId = TableId.tableId(record.topic());
        return parseSchema(record.value(), tableId);
    }

    @Override
    public Optional<Tuple2<TableId, Schema>> parseRecordKeySchema(
            ConsumerRecord<byte[], byte[]> record) {
        TableId tableId = TableId.tableId(record.topic());
        return parseSchema(record.key(), tableId);
    }

    public Optional<Tuple2<TableId, Schema>> parseSchema(byte[] message, TableId tableId) {
        if (message == null || message.length == 0) {
            return Optional.empty();
        }
        try {
            SourceRecord valueSourceRecord = getSourceRecordByJson(message);
            Schema valueSchema = SchemaUtils.convert(valueSourceRecord.getSchema());
            return Optional.of(Tuple2.of(tableId, valueSchema));
        } catch (Exception e) {
            LOG.debug("Failed to parse schema by kafka record.", e);
            return Optional.empty();
        }
    }

    private SourceRecord getSourceRecordByJson(byte[] bytes) throws IOException {
        JsonParser root = objectMapper.getFactory().createParser(bytes);
        if (root.currentToken() == null) {
            root.nextToken();
        }
        return jsonToSourceRecordConverter.convert(root);
    }
}
