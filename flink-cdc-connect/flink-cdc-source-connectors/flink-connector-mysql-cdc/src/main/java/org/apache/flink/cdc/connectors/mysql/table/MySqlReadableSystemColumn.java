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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * The system column is similar to the metadata, which is used to describe the record properties.
 * The difference between the metadata: 1) Users can't specify the system data, which is generated
 * by the catalog if the specified table is a sharding table; 2) System column works like the
 * physical column and it works as primary key now.
 */
public enum MySqlReadableSystemColumn {

    /** Name of the database that contain the row. */
    DATABASE_NAME(
            0,
            "_db_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.DATABASE_NAME_KEY));
                }
            }),

    /** Name of the table that contain the row. . */
    TABLE_NAME(
            1,
            "_table_name",
            DataTypes.STRING().notNull(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(SourceRecord record) {
                    Struct messageStruct = (Struct) record.value();
                    Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
                    return StringData.fromString(
                            sourceStruct.getString(AbstractSourceInfo.TABLE_NAME_KEY));
                }
            });

    private final int index;

    private final String key;

    private final DataType dataType;

    private final MetadataConverter converter;

    MySqlReadableSystemColumn(
            int index, String key, DataType dataType, MetadataConverter converter) {
        this.index = index;
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public int getIndex() {
        return index;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }
}
