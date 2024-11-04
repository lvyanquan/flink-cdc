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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SchemaEvolveManager}. */
public class SchemaEvolveManagerTest {

    private static final TableId TABLE_ID = TableId.tableId("test-db", "test-table");

    private static final Schema SCHEMA_BASE =
            Schema.newBuilder()
                    .physicalColumn("f0", DataTypes.INT().notNull())
                    .physicalColumn("f1", DataTypes.STRING())
                    .physicalColumn("f2", DataTypes.DOUBLE())
                    .primaryKey("f0")
                    .build();

    @Test
    public void testCompatibleAsIs() {
        SchemaEvolveResult result =
                SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, SCHEMA_BASE);
        assertThat(result.isCompatibleAsIs()).isTrue();

        Schema schema1 =
                Schema.newBuilder()
                        .physicalColumn("f2", DataTypes.DOUBLE())
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f0", DataTypes.INT().notNull())
                        .primaryKey("f0")
                        .build();
        result = SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, schema1);
        assertThat(result.isCompatibleAsIs()).isTrue();

        Schema schema2 =
                Schema.newBuilder()
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f0", DataTypes.INT().notNull())
                        .primaryKey("f0")
                        .build();
        result = SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, schema2);
        assertThat(result.isCompatibleAsIs()).isTrue();
    }

    @Test
    public void testPrimaryKeyChange() {
        // rename pk col
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();
        SchemaEvolveResult result =
                SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, newSchema);
        assertThat(result.isIncompatible()).isTrue();
        assertThat(result.getIncompatibleReason()).isEqualTo("Primary keys should not be changed.");

        // remove pk
        newSchema =
                Schema.newBuilder()
                        .physicalColumn("f0", DataTypes.INT().notNull())
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f2", DataTypes.DOUBLE())
                        .build();
        result = SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, newSchema);
        assertThat(result.isIncompatible()).isTrue();
        assertThat(result.getIncompatibleReason()).isEqualTo("Primary keys should not be changed.");
    }

    @Test
    public void testPrimaryKeyTypeChange() {
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("f0", DataTypes.BIGINT().notNull())
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f2", DataTypes.DOUBLE())
                        .primaryKey("f0")
                        .build();
        SchemaEvolveResult result =
                SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, newSchema);
        assertThat(result.isCompatibleAfterEvolution()).isTrue();
        assertThat(result.getSchemaAfterEvolve()).isEqualTo(newSchema);
        assertThat(result.getSchemaChanges())
                .containsExactly(
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("f0", DataTypes.BIGINT().notNull()),
                                Collections.singletonMap("f0", DataTypes.INT().notNull())));
    }

    @Test
    public void testAddMultipleColumn() {
        List<Column> newColumns = new ArrayList<>(SCHEMA_BASE.getColumns());
        newColumns.add(Column.physicalColumn("f3", DataTypes.BOOLEAN()));
        newColumns.add(Column.physicalColumn("f4", DataTypes.TIMESTAMP(3)));
        newColumns.add(Column.physicalColumn("f5", DataTypes.DATE()));
        Schema newSchema = SCHEMA_BASE.copy(newColumns);

        SchemaEvolveResult result =
                SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, newSchema);
        assertThat(result.isCompatibleAfterEvolution()).isTrue();
        assertThat(result.getSchemaAfterEvolve()).isEqualTo(newSchema);
        assertThat(result.getSchemaChanges())
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Arrays.asList(
                                        AddColumnEvent.last(
                                                Column.physicalColumn("f3", DataTypes.BOOLEAN())),
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "f4", DataTypes.TIMESTAMP(3))),
                                        AddColumnEvent.last(
                                                Column.physicalColumn("f5", DataTypes.DATE())))));
    }

    @Test
    public void testMultipleSchemaChangeEvents() {
        Schema newSchema =
                Schema.newBuilder()
                        .physicalColumn("f0", DataTypes.STRING().notNull())
                        .physicalColumn("f2", DataTypes.INT())
                        .physicalColumn("f3", DataTypes.DECIMAL(10, 10))
                        .physicalColumn("f4", DataTypes.TIMESTAMP_LTZ())
                        .primaryKey("f0")
                        .build();
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("f0", DataTypes.STRING().notNull())
                        .physicalColumn("f1", DataTypes.STRING())
                        .physicalColumn("f2", DataTypes.DOUBLE())
                        .physicalColumn("f3", DataTypes.DECIMAL(10, 10))
                        .physicalColumn("f4", DataTypes.TIMESTAMP_LTZ())
                        .primaryKey("f0")
                        .build();

        SchemaEvolveResult result =
                SchemaEvolveManager.evolveSchema(TABLE_ID, SCHEMA_BASE, newSchema);
        assertThat(result.isCompatibleAfterEvolution()).isTrue();
        assertThat(result.getSchemaAfterEvolve()).isEqualTo(expectedSchema);
        assertThat(result.getSchemaChanges())
                .containsExactlyInAnyOrder(
                        new AddColumnEvent(
                                TABLE_ID,
                                Arrays.asList(
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "f3", DataTypes.DECIMAL(10, 10))),
                                        AddColumnEvent.last(
                                                Column.physicalColumn(
                                                        "f4", DataTypes.TIMESTAMP_LTZ())))),
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("f0", DataTypes.STRING().notNull()),
                                Collections.singletonMap("f0", DataTypes.INT().notNull())));
    }
}
