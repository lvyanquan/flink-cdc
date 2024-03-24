/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Copied from https://github.com/osheroff/mysql-binlog-connector-java project of 0.27.2 version.
 *
 * <p>Line 40: Set visibility of {@link #mayContainExtraInformation} from private to protected.
 *
 * <p>Line 69: Set visibility of {@link #deserializeRows} from private to protected.
 */
public class WriteRowsEventDataDeserializer
        extends AbstractRowsEventDataDeserializer<WriteRowsEventData> {

    protected boolean mayContainExtraInformation;

    public WriteRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
        super(tableMapEventByTableId);
    }

    public WriteRowsEventDataDeserializer setMayContainExtraInformation(
            boolean mayContainExtraInformation) {
        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }

    @Override
    public WriteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        WriteRowsEventData eventData = new WriteRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
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

    protected List<Serializable[]> deserializeRows(
            long tableId, BitSet includedColumns, ByteArrayInputStream inputStream)
            throws IOException {
        List<Serializable[]> result = new LinkedList<Serializable[]>();
        while (inputStream.available() > 0) {
            result.add(deserializeRow(tableId, includedColumns, inputStream));
        }
        return result;
    }
}
