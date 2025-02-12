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

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Spec to describe the relation between the table in Flink and collection in MongoDB. */
public class MongoDBTableSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ObjectPath tablePathInFlink;
    private final DataType physicalDataType;
    @Nullable private final String database;
    @Nullable private final String collection;

    private List<String> metadataKeys;

    public MongoDBTableSpec(
            ObjectPath tablePathInFlink,
            DataType physicalDataType,
            String database,
            String collection) {
        this.tablePathInFlink = tablePathInFlink;
        this.physicalDataType = physicalDataType;
        this.database = database;
        this.collection = collection;
        this.metadataKeys = new ArrayList<>();
    }

    public ObjectPath getTablePathInFlink() {
        return tablePathInFlink;
    }

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    List<String> getMetadataKeys() {
        return metadataKeys;
    }

    public void setMetadataKeys(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoDBTableSpec)) {
            return false;
        }
        MongoDBTableSpec that = (MongoDBTableSpec) o;
        return Objects.equals(tablePathInFlink, that.tablePathInFlink)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection)
                && Objects.equals(metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePathInFlink, physicalDataType, database, collection, metadataKeys);
    }

    @Override
    public String toString() {
        return String.format(
                "{ tablePathInFlink: %s, database: %s, collection: %s, physicalDataType: %s, metadataKeys: %s }",
                tablePathInFlink.getFullName(),
                database,
                collection,
                physicalDataType,
                metadataKeys);
    }
}
