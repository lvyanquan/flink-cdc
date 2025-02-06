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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link SinkWriter} for Apache Iceberg. */
public class IcebergWriter implements CommittingSinkWriter<Event, WriteResultWrapper> {

    Map<TableId, RowDataTaskWriterFactory> writerFactoryMap;

    Map<TableId, TaskWriter<RowData>> writerMap;

    Map<TableId, TableSchemaWrapper> schemaMap;

    private final Catalog catalog;

    private final int taskId;

    private final int attemptId;

    private final ZoneId zoneId;

    public IcebergWriter(
            Map<String, String> catalogOptions, int taskId, int attemptId, ZoneId zoneId) {
        catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
        writerFactoryMap = new HashMap<>();
        writerMap = new HashMap<>();
        schemaMap = new HashMap<>();
        this.taskId = taskId;
        this.attemptId = attemptId;
        this.zoneId = zoneId;
    }

    @Override
    public Collection<WriteResultWrapper> prepareCommit() throws IOException, InterruptedException {
        List<WriteResultWrapper> list = new ArrayList<>();
        for (Map.Entry<TableId, TaskWriter<RowData>> entry : writerMap.entrySet()) {
            WriteResult writeResult = entry.getValue().complete();
            list.add(new WriteResultWrapper(writeResult, entry.getKey()));
        }
        return list;
    }

    private RowDataTaskWriterFactory getRowDataTaskWriterFactory(TableId tableId) {
        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
        RowType rowType = FlinkSchemaUtil.convert(table.schema());
        RowDataTaskWriterFactory rowDataTaskWriterFactory =
                new RowDataTaskWriterFactory(
                        table,
                        rowType,
                        1024 * 1024,
                        FileFormat.PARQUET,
                        new HashMap<>(),
                        new ArrayList<>(table.schema().identifierFieldIds()),
                        true);
        rowDataTaskWriterFactory.initialize(taskId, attemptId);
        return rowDataTaskWriterFactory;
    }

    @Override
    public void write(Event event, Context context) throws IOException, InterruptedException {
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();
            writerFactoryMap.computeIfAbsent(tableId, this::getRowDataTaskWriterFactory);
            TaskWriter<RowData> writer =
                    writerMap.computeIfAbsent(
                            tableId, tableId1 -> writerFactoryMap.get(tableId1).create());
            writer.write(schemaMap.get(tableId).convertEventToRowData(dataChangeEvent));
        } else {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);

            Schema newSchema =
                    tableSchemaWrapper != null
                            ? SchemaUtils.applySchemaChangeEvent(
                                    tableSchemaWrapper.getSchema(), schemaChangeEvent)
                            : SchemaUtils.applySchemaChangeEvent(null, schemaChangeEvent);

            schemaMap.put(tableId, new TableSchemaWrapper(newSchema, zoneId));
            writerFactoryMap.remove(tableId);
            writerMap.remove(tableId);
        }
    }

    @Override
    public void flush(boolean flush) {}

    @Override
    public void writeWatermark(Watermark watermark) {}

    @Override
    public void close() throws Exception {
        if (schemaMap != null) {
            schemaMap.clear();
            schemaMap = null;
        }
        if (writerMap != null) {
            for (TaskWriter<RowData> writer : writerMap.values()) {
                writer.close();
            }
            writerMap.clear();
            writerMap = null;
        }
        if (writerFactoryMap != null) {
            writerFactoryMap.clear();
            writerFactoryMap = null;
        }
    }
}
