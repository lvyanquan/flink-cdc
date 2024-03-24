/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.Map;

/** Use table filter to skip some deserialization. */
public class WriteRowsWithFilterDeserializer extends RowDeserializers.WriteRowsDeserializer {

    private final MySqlDatabaseSchema schema;

    public WriteRowsWithFilterDeserializer(
            Map<Long, TableMapEventData> tableMapEventByTableId, MySqlDatabaseSchema schema) {
        super(tableMapEventByTableId);
        this.schema = schema;
    }

    @Override
    public WriteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        long tableId = inputStream.readLong(6);
        if (schema.getTableId(tableId) == null) {
            // null EventData means that this event will be skipped in
            // MySqlStreamingChangeEventSource#handleChange.
            return null;
        }
        WriteRowsEventData eventData = new WriteRowsEventData();
        eventData.setTableId(tableId);
        inputStream.skip(2); // reserved
        if (mayContainExtraInformation) {
            int extraInfoLength = inputStream.readInteger(2);
            inputStream.skip(extraInfoLength - 2);
        }
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
        eventData.setRows(
                deserializeRows(
                        eventData.getTableId(), eventData.getIncludedColumns(), inputStream));
        return eventData;
    }
}
