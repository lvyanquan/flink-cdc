/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.debezium.dispatcher;

import io.debezium.connector.mysql.MySqlChangeRecordEmitter;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Struct;

/**
 * Class for parallel processing of {@link ChangeRecordEventHandler} and Single threaded processing
 * of {@link ChangeRecordEventConsumer}.
 */
public class ChangeRecordEvent<T extends DataCollectionId> {
    MySqlChangeRecordEmitter changeRecordEmitter;
    TableSchema tableSchema;
    T dataCollectionId;
    Struct newKey;
    Struct oldKey;
    Struct newValue;
    Struct oldValue;
    Struct readEnvelope;
    Struct createEnvelope;
    Struct deleteEnvelope;
    Struct updateEnvelope;

    public MySqlPartition getPartition() {
        return partition;
    }

    public void setPartition(MySqlPartition partition) {
        this.partition = partition;
    }

    MySqlPartition partition;

    public MySqlChangeRecordEmitter getChangeRecordEmitter() {
        return changeRecordEmitter;
    }

    public void setChangeRecordEmitter(MySqlChangeRecordEmitter changeRecordEmitter) {
        this.changeRecordEmitter = changeRecordEmitter;
    }

    public Struct getOldKey() {
        return oldKey;
    }

    public void setOldKey(Struct oldKey) {
        this.oldKey = oldKey;
    }

    public Struct getNewKey() {
        return newKey;
    }

    public void setNewKey(Struct newKey) {
        this.newKey = newKey;
    }

    public Struct getNewValue() {
        return newValue;
    }

    public void setNewValue(Struct newValue) {
        this.newValue = newValue;
    }

    public Struct getOldValue() {
        return oldValue;
    }

    public void setOldValue(Struct oldValue) {
        this.oldValue = oldValue;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public T getDataCollectionId() {
        return dataCollectionId;
    }

    public void setDataCollectionId(T dataCollectionId) {
        this.dataCollectionId = dataCollectionId;
    }

    public Struct getReadEnvelope() {
        return readEnvelope;
    }

    public void setReadEnvelope(Struct readEnvelope) {
        this.readEnvelope = readEnvelope;
    }

    public Struct getCreateEnvelope() {
        return createEnvelope;
    }

    public void setCreateEnvelope(Struct createEnvelope) {
        this.createEnvelope = createEnvelope;
    }

    public Struct getDeleteEnvelope() {
        return deleteEnvelope;
    }

    public void setDeleteEnvelope(Struct deleteEnvelope) {
        this.deleteEnvelope = deleteEnvelope;
    }

    public Struct getUpdateEnvelope() {
        return updateEnvelope;
    }

    public void setUpdateEnvelope(Struct updateEnvelope) {
        this.updateEnvelope = updateEnvelope;
    }

    /** Clear schema info and others for GC purpose. */
    public void clearAll() {
        this.changeRecordEmitter = null;
        this.tableSchema = null;
        this.oldKey = null;
        this.newKey = null;
        this.oldValue = null;
        this.newValue = null;
        this.readEnvelope = null;
        this.createEnvelope = null;
        this.deleteEnvelope = null;
        this.updateEnvelope = null;
    }
}
