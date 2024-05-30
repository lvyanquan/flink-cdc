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

package org.apache.flink.cdc.connectors.values.sink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An e2e {@link SinkFunction} implementation that print all {@link DataChangeEvent} out. */
public class ValuesDataSinkFunction implements SinkFunction<Event> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValuesDataSinkFunction.class);

    private final boolean materializedInMemory;

    private final boolean print;

    private final boolean stdErr;

    private final boolean logger;

    private final long limit;
    private long printCnt = 0;
    private boolean exceedLimitWarned = false;

    /**
     * keep the relationship of TableId and Schema as write method may rely on the schema
     * information of DataChangeEvent.
     */
    private final Map<TableId, Schema> schemaMaps;

    private final Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps;

    public ValuesDataSinkFunction(
            boolean materializedInMemory,
            boolean print,
            boolean stdErr,
            boolean logger,
            long limit) {
        this.materializedInMemory = materializedInMemory;
        this.print = print;
        this.stdErr = stdErr;
        this.logger = logger;
        this.limit = limit;
        schemaMaps = new HashMap<>();
        fieldGetterMaps = new HashMap<>();
    }

    @Override
    public void invoke(Event event, Context context) throws Exception {
        if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            if (event instanceof CreateTableEvent) {
                Schema schema = ((CreateTableEvent) event).getSchema();
                schemaMaps.put(tableId, schema);
                fieldGetterMaps.put(tableId, SchemaUtils.createFieldGetters(schema));
            } else {
                if (!schemaMaps.containsKey(tableId)) {
                    throw new RuntimeException("schema of " + tableId + " is not existed.");
                }
                Schema schema =
                        SchemaUtils.applySchemaChangeEvent(
                                schemaMaps.get(tableId), schemaChangeEvent);
                schemaMaps.put(tableId, schema);
                fieldGetterMaps.put(tableId, SchemaUtils.createFieldGetters(schema));
            }
        } else if (materializedInMemory && event instanceof DataChangeEvent) {
            ValuesDatabase.applyDataChangeEvent((DataChangeEvent) event);
        }

        // print the detail message to console for verification.
        if (print) {
            if (printCnt < limit) {
                String output =
                        ValuesDataSinkHelper.convertEventToStr(
                                event, fieldGetterMaps.get(((ChangeEvent) event).tableId()));

                PrintStream stream = !stdErr ? System.out : System.err;
                stream.println(output);

                if (logger) {
                    LOGGER.info(output);
                }
                printCnt++;
            } else if (!exceedLimitWarned) {
                exceedLimitWarned = true;
                LOGGER.warn(
                        "The number of events exceeds the values sink limit, and the rest rows will not be output. "
                                + "Please increase the value for option 'sink.limit' in the values sink to display more events.");
            }
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        SinkFunction.super.writeWatermark(watermark);
    }

    @Override
    public void finish() throws Exception {
        SinkFunction.super.finish();
    }
}
