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

package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Copied from https://github.com/osheroff/mysql-binlog-connector-java project of 0.27.2 version.
 *
 * <p>Line 41: Set visibility of {@link #mayContainExtraInformation} from private to protected.
 *
 * <p>Line 69: Set visibility of {@link #deserializeRows} from private to protected.
 */
public class UpdateRowsEventDataDeserializer
        extends AbstractRowsEventDataDeserializer<UpdateRowsEventData> {

    protected boolean mayContainExtraInformation;

    public UpdateRowsEventDataDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
        super(tableMapEventByTableId);
    }

    public UpdateRowsEventDataDeserializer setMayContainExtraInformation(
            boolean mayContainExtraInformation) {
        this.mayContainExtraInformation = mayContainExtraInformation;
        return this;
    }

    @Override
    public UpdateRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        UpdateRowsEventData eventData = new UpdateRowsEventData();
        eventData.setTableId(inputStream.readLong(6));
        inputStream.skip(2); // reserved
        if (mayContainExtraInformation) {
            int extraInfoLength = inputStream.readInteger(2);
            inputStream.skip(extraInfoLength - 2);
        }
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setIncludedColumnsBeforeUpdate(inputStream.readBitSet(numberOfColumns, true));
        eventData.setIncludedColumns(inputStream.readBitSet(numberOfColumns, true));
        eventData.setRows(deserializeRows(eventData, inputStream));
        return eventData;
    }

    protected List<Map.Entry<Serializable[], Serializable[]>> deserializeRows(
            UpdateRowsEventData eventData, ByteArrayInputStream inputStream) throws IOException {
        long tableId = eventData.getTableId();
        BitSet includedColumnsBeforeUpdate = eventData.getIncludedColumnsBeforeUpdate(),
                includedColumns = eventData.getIncludedColumns();
        List<Map.Entry<Serializable[], Serializable[]>> rows =
                new ArrayList<Map.Entry<Serializable[], Serializable[]>>();
        while (inputStream.available() > 0) {
            rows.add(
                    new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(
                            deserializeRow(tableId, includedColumnsBeforeUpdate, inputStream),
                            deserializeRow(tableId, includedColumns, inputStream)));
        }
        return rows;
    }
}
