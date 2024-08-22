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

package org.apache.flink.cdc.connectors.hologres;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy;
import org.apache.flink.cdc.connectors.hologres.schema.HologresTypes;
import org.apache.flink.cdc.connectors.hologres.sink.HologresMetadataApplier;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParamBuilder;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Test for HologresMetadataApplier. */
public class HologresMetadataApplierTest extends HologresTestBase {

    public HologresMetadataApplierTest() throws IOException {
        super();
    }

    @Before
    public void setup() {
        this.sinkTable = "test_meta_applier" + "_" + randomSuffix;
    }

    @Test
    public void testSchemaChange() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("a", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("b", tableSchema.getColumn(1).getName());
            Assert.assertEquals(Types.VARCHAR, tableSchema.getColumn(1).getType());
            Assert.assertTrue(tableSchema.getColumn(1).getAllowNull());

            // 2. add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN())));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);

            tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("c", tableSchema.getColumn(2).getName());
            Assert.assertEquals(Types.DOUBLE, tableSchema.getColumn(2).getType());
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            Assert.assertEquals("d", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());

            //  3. rename columns
            Map<String, String> nameMapping = new HashMap<>();
            nameMapping.put("a", "a1");
            nameMapping.put("d", "d1");
            RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
            applier.applySchemaChange(renameColumnEvent);
            tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("a1", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("d1", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testSchemaChangeNotPublicSchema() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "test", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema("test." + sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("test", tableSchema.getSchemaName());
            Assert.assertEquals("a", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("b", tableSchema.getColumn(1).getName());
            Assert.assertEquals(Types.VARCHAR, tableSchema.getColumn(1).getType());
            Assert.assertTrue(tableSchema.getColumn(1).getAllowNull());

            // add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN())));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);

            tableSchema = holoClient.getTableSchema("test." + sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("test", tableSchema.getSchemaName());
            Assert.assertEquals("c", tableSchema.getColumn(2).getName());
            Assert.assertEquals(Types.DOUBLE, tableSchema.getColumn(2).getType());
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            Assert.assertEquals("d", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());
            //  3. rename columns
            Map<String, String> nameMapping = new HashMap<>();
            nameMapping.put("a", "a1");
            nameMapping.put("d", "d1");
            RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
            applier.applySchemaChange(renameColumnEvent);
            tableSchema = holoClient.getTableSchema("test." + sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("test", tableSchema.getSchemaName());
            Assert.assertEquals("a1", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("d1", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());

        } finally {
            dropTable("test." + sinkTable);
        }
    }

    @Test
    public void testCreateTableAllTypes() {
        Object[][] allTypeMapping =
                new Object[][] {
                    new Object[] {
                        "a", DataTypes.CHAR(10).notNull(), HologresTypes.PG_CHAR, Types.CHAR
                    },
                    new Object[] {
                        "b",
                        DataTypes.VARCHAR(10),
                        HologresTypes.PG_CHARACTER_VARYING,
                        Types.VARCHAR
                    },
                    new Object[] {"c", DataTypes.STRING(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    // PG_BOOLEAN -> Types.BIT
                    new Object[] {"d", DataTypes.BOOLEAN(), HologresTypes.PG_BOOLEAN, Types.BIT},
                    new Object[] {"e", DataTypes.BINARY(10), HologresTypes.PG_BYTEA, Types.BINARY},
                    new Object[] {
                        "f", DataTypes.VARBINARY(10), HologresTypes.PG_BYTEA, Types.BINARY
                    },
                    new Object[] {"g", DataTypes.BYTES(), HologresTypes.PG_BYTEA, Types.BINARY},
                    // PG_NUMERIC -> Types.NUMERIC
                    new Object[] {
                        "h", DataTypes.DECIMAL(5, 2), HologresTypes.PG_NUMERIC, Types.NUMERIC
                    },
                    new Object[] {
                        "i", DataTypes.TINYINT(), HologresTypes.PG_SMALLINT, Types.SMALLINT
                    },
                    new Object[] {
                        "j", DataTypes.SMALLINT(), HologresTypes.PG_SMALLINT, Types.SMALLINT
                    },
                    new Object[] {"k", DataTypes.INT(), HologresTypes.PG_INTEGER, Types.INTEGER},
                    new Object[] {"l", DataTypes.BIGINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"m", DataTypes.FLOAT(), HologresTypes.PG_REAL, Types.REAL},
                    new Object[] {
                        "n", DataTypes.DOUBLE(), HologresTypes.PG_DOUBLE_PRECISION, Types.DOUBLE
                    },
                    new Object[] {"o", DataTypes.DATE(), HologresTypes.PG_DATE, Types.DATE},
                    new Object[] {"p", DataTypes.TIME(5), HologresTypes.PG_TIME, Types.TIME},
                    new Object[] {
                        "q", DataTypes.TIMESTAMP(), HologresTypes.PG_TIMESTAMP, Types.TIMESTAMP
                    },
                    new Object[] {
                        "r", DataTypes.TIMESTAMP_TZ(), HologresTypes.PG_TIMESTAMPTZ, Types.TIMESTAMP
                    },
                    new Object[] {
                        "s",
                        DataTypes.TIMESTAMP_LTZ(),
                        HologresTypes.PG_TIMESTAMPTZ,
                        Types.TIMESTAMP
                    },
                    new Object[] {
                        "boolean_array",
                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
                        HologresTypes.PG_BOOLEAN_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "int_array",
                        DataTypes.ARRAY(DataTypes.INT()),
                        HologresTypes.PG_INTEGER_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "bigint_array",
                        DataTypes.ARRAY(DataTypes.BIGINT()),
                        HologresTypes.PG_BIGINT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "float_array",
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        HologresTypes.PG_REAL_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "double_array",
                        DataTypes.ARRAY(DataTypes.DOUBLE()),
                        HologresTypes.PG_DOUBLE_PRECISION_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "char_array",
                        DataTypes.ARRAY(DataTypes.CHAR(2)),
                        HologresTypes.PG_CHAR_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "varchar_array",
                        DataTypes.ARRAY(DataTypes.VARCHAR(2)),
                        HologresTypes.PG_CHARACTER_VARYING_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "text_array",
                        DataTypes.ARRAY(DataTypes.STRING()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    }
                };

        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema.Builder builder = Schema.newBuilder();
            for (int i = 0; i < allTypeMapping.length; i++) {
                builder.physicalColumn(
                        (String) allTypeMapping[i][0], (DataType) allTypeMapping[i][1]);
            }

            Schema schema = builder.build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            for (int i = 0; i < allTypeMapping.length; i++) {
                Assert.assertEquals(allTypeMapping[i][0], tableSchema.getColumn(i).getName());
                Assert.assertEquals(allTypeMapping[i][2], tableSchema.getColumn(i).getTypeName());
                Assert.assertEquals(allTypeMapping[i][3], tableSchema.getColumn(i).getType());
            }

        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testCreateTableWithDecimalPk() throws HoloClientException {
        String table1 = "decimal_1_" + sinkTable;
        String table2 = "decimal_2_" + sinkTable;
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table1 with a decimal column as non-pk
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.DECIMAL(20, 0))
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", table1);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(table1, true);
            Assert.assertEquals(table1, tableSchema.getTableName());
            Assert.assertEquals("b", tableSchema.getColumn(1).getName());
            Assert.assertEquals(Types.NUMERIC, tableSchema.getColumn(1).getType());

            // 2. create table2 with a decimal column as non-pk
            Schema schema2 =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.DECIMAL(20, 0))
                            .primaryKey("b")
                            .build();
            TableId tableId2 = TableId.tableId("default_namespace", "public", table2);
            CreateTableEvent createTableEvent2 = new CreateTableEvent(tableId2, schema2);
            applier.applySchemaChange(createTableEvent2);

            TableSchema tableSchema2 = holoClient.getTableSchema(table2, true);
            Assert.assertEquals(table2, tableSchema2.getTableName());
            Assert.assertEquals("b", tableSchema2.getColumn(1).getName());
            Assert.assertEquals(Types.VARCHAR, tableSchema2.getColumn(1).getType());

        } finally {
            dropTable(table1);
            dropTable(table2);
        }
    }

    @Test
    public void testCreatePartitionTable() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING().notNull())
                            .primaryKey("a", "b")
                            .partitionKey("b")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("a", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("b", tableSchema.getColumn(1).getName());
            Assert.assertEquals(Types.VARCHAR, tableSchema.getColumn(1).getType());
            // hologres primary key is not null
            Assert.assertTrue(!tableSchema.getColumn(1).getAllowNull());

            Assert.assertEquals("b", tableSchema.getPartitionInfo());

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testCreateTableWithTableOption() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            Map<String, String> tableOptions = new HashMap<>();
            tableOptions.put("distribution_key", "b");
            tableOptions.put("orientation", "row");
            tableOptions.put("CLUSTERING_KEY", "a");
            HologresMetadataApplier applier =
                    getHologresMetadataApplier(tableOptions, TypeNormalizationStrategy.STANDARD);

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a", "b")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("a", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertTrue(!tableSchema.getColumn(0).getAllowNull());

            Assert.assertEquals("b", tableSchema.getColumn(1).getName());
            Assert.assertEquals(Types.VARCHAR, tableSchema.getColumn(1).getType());
            // primary key will be created as not null by default.
            Assert.assertTrue(!tableSchema.getColumn(1).getAllowNull());

            Assert.assertEquals(new String[] {"a", "b"}, tableSchema.getPrimaryKeys());
            Assert.assertEquals(new String[] {"b"}, tableSchema.getDistributionKeys());
            Assert.assertEquals(new String[] {"a:asc"}, tableSchema.getClusteringKey());
            Assert.assertEquals("row", tableSchema.getOrientation());

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testCreatePartitionTableWhenPatitionKeyNotInPks() {
        TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
        try {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING().notNull())
                            .primaryKey("a")
                            .partitionKey("b")
                            .build();
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);
        } catch (Exception ex) {
            Assertions.assertThat(ex)
                    .rootCause()
                    .hasMessageContaining(
                            String.format(
                                    "PRIMARY KEY constraint on table \"%s\" lacks column \"%s\" which is part of the partition key",
                                    tableId, "b"));
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testAddColumnNotNull() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN().notNull())));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals("c", tableSchema.getColumn(2).getName());
            Assert.assertEquals(Types.DOUBLE, tableSchema.getColumn(2).getType());
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            // NOT NULL will be applied as nullable
            Assert.assertEquals("d", tableSchema.getColumn(3).getName());
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testAddColumnWithPosition() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE()),
                            AddColumnEvent.ColumnPosition.FIRST,
                            null));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN().notNull()),
                            AddColumnEvent.ColumnPosition.BEFORE,
                            "a"));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);
            // Add column with position will be added in last position.
            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals("c", tableSchema.getColumn(2).getName());
            Assert.assertEquals("d", tableSchema.getColumn(3).getName());

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testDropColumn() {
        try {
            HologresMetadataApplier applier = getHologresMetadataApplier();
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // drop column
            DropColumnEvent dropColumnEvent = new DropColumnEvent(tableId, Arrays.asList("b"));
            applier.applySchemaChange(dropColumnEvent);
            throw new Exception("If throw this exception, mean that job execute successfully.");
        } catch (Exception ex) {
            Assert.assertTrue(
                    "Hologres not support drop columnType now",
                    ex.getMessage().contains("Hologres not support drop columnType now"));
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testAlterColumn() {
        try {
            HologresMetadataApplier applier = getHologresMetadataApplier();
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // alter column
            HashMap<String, DataType> dataTypeHashMap = new HashMap<>();
            dataTypeHashMap.put("b", DataTypes.VARBINARY(2));
            AlterColumnTypeEvent alterColumnTypeEvent =
                    new AlterColumnTypeEvent(tableId, dataTypeHashMap);
            applier.applySchemaChange(alterColumnTypeEvent);
            throw new Exception("If throw this exception, mean that job execute successfully.");
        } catch (Exception ex) {
            Assertions.assertThat(ex)
                    .rootCause()
                    .hasMessageContaining("Hologres not support alter columnType now");

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testCreateTableAllTypeWithOnlyBroadenStrategy() {
        Object[][] allTypeMapping =
                new Object[][] {
                    new Object[] {
                        "a", DataTypes.CHAR(10).notNull(), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {"b", DataTypes.VARCHAR(10), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"c", DataTypes.STRING(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    // PG_BOOLEAN -> Types.BIT
                    new Object[] {"d", DataTypes.BOOLEAN(), HologresTypes.PG_BOOLEAN, Types.BIT},
                    new Object[] {"e", DataTypes.BINARY(10), HologresTypes.PG_BYTEA, Types.BINARY},
                    new Object[] {
                        "f", DataTypes.VARBINARY(10), HologresTypes.PG_BYTEA, Types.BINARY
                    },
                    new Object[] {"g", DataTypes.BYTES(), HologresTypes.PG_BYTEA, Types.BINARY},
                    // PG_NUMERIC -> Types.NUMERIC
                    new Object[] {
                        "h", DataTypes.DECIMAL(5, 2), HologresTypes.PG_NUMERIC, Types.NUMERIC
                    },
                    new Object[] {"i", DataTypes.TINYINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"j", DataTypes.SMALLINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"k", DataTypes.INT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"l", DataTypes.BIGINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {
                        "m", DataTypes.FLOAT(), HologresTypes.PG_DOUBLE_PRECISION, Types.DOUBLE
                    },
                    new Object[] {
                        "n", DataTypes.DOUBLE(), HologresTypes.PG_DOUBLE_PRECISION, Types.DOUBLE
                    },
                    new Object[] {"o", DataTypes.DATE(), HologresTypes.PG_DATE, Types.DATE},
                    new Object[] {"p", DataTypes.TIME(5), HologresTypes.PG_TIME, Types.TIME},
                    new Object[] {
                        "q", DataTypes.TIMESTAMP(), HologresTypes.PG_TIMESTAMP, Types.TIMESTAMP
                    },
                    new Object[] {
                        "r", DataTypes.TIMESTAMP_TZ(), HologresTypes.PG_TIMESTAMPTZ, Types.TIMESTAMP
                    },
                    new Object[] {
                        "s",
                        DataTypes.TIMESTAMP_LTZ(),
                        HologresTypes.PG_TIMESTAMPTZ,
                        Types.TIMESTAMP
                    },
                    new Object[] {
                        "boolean_array",
                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
                        HologresTypes.PG_BOOLEAN_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "int_array",
                        DataTypes.ARRAY(DataTypes.INT()),
                        HologresTypes.PG_BIGINT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "bigint_array",
                        DataTypes.ARRAY(DataTypes.BIGINT()),
                        HologresTypes.PG_BIGINT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "float_array",
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        HologresTypes.PG_DOUBLE_PRECISION_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "double_array",
                        DataTypes.ARRAY(DataTypes.DOUBLE()),
                        HologresTypes.PG_DOUBLE_PRECISION_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "char_array",
                        DataTypes.ARRAY(DataTypes.CHAR(2)),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "varchar_array",
                        DataTypes.ARRAY(DataTypes.VARCHAR(2)),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "text_array",
                        DataTypes.ARRAY(DataTypes.STRING()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    }
                };

        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier =
                    getHologresMetadataApplier(new HashMap<>(), TypeNormalizationStrategy.BROADEN);

            // 1. create table
            Schema.Builder builder = Schema.newBuilder();
            for (int i = 0; i < allTypeMapping.length; i++) {
                builder.physicalColumn(
                        (String) allTypeMapping[i][0], (DataType) allTypeMapping[i][1]);
            }

            Schema schema = builder.build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            for (int i = 0; i < allTypeMapping.length; i++) {
                Assert.assertEquals(allTypeMapping[i][0], tableSchema.getColumn(i).getName());
                Assert.assertEquals(allTypeMapping[i][2], tableSchema.getColumn(i).getTypeName());
                Assert.assertEquals(allTypeMapping[i][3], tableSchema.getColumn(i).getType());
            }

            // apply change when enable TypeNormalization

        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testAddColumnWithOnlyBroadenStrategy() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier =
                    getHologresMetadataApplier(new HashMap<>(), TypeNormalizationStrategy.BROADEN);
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.TINYINT())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.CHAR(4))));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            // TINYINT -> PG_BIGINT
            Assert.assertEquals(tableSchema.getColumn(2).getName(), "c");
            Assert.assertEquals(tableSchema.getColumn(2).getType(), Types.BIGINT);
            Assert.assertEquals(tableSchema.getColumn(2).getTypeName(), HologresTypes.PG_BIGINT);
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            // CHAR -> PG_TEXT
            Assert.assertEquals(tableSchema.getColumn(3).getName(), "d");
            Assert.assertEquals(tableSchema.getColumn(3).getType(), Types.VARCHAR);
            Assert.assertEquals(tableSchema.getColumn(3).getTypeName(), HologresTypes.PG_TEXT);
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());
        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testAlterColumnWithOnlyBroadenStrategy() {

        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier =
                    getHologresMetadataApplier(new HashMap<>(), TypeNormalizationStrategy.BROADEN);

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .physicalColumn("c", DataTypes.FLOAT())
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            // INT -> PG_BIGINT
            Assert.assertEquals(tableSchema.getColumn(0).getName(), "a");
            Assert.assertEquals(tableSchema.getColumn(0).getType(), Types.BIGINT);
            Assert.assertEquals(tableSchema.getColumn(0).getTypeName(), HologresTypes.PG_BIGINT);

            // STRING -> PG_TEXT
            Assert.assertEquals(tableSchema.getColumn(1).getName(), "b");
            Assert.assertEquals(tableSchema.getColumn(1).getType(), Types.VARCHAR);
            Assert.assertEquals(tableSchema.getColumn(1).getTypeName(), HologresTypes.PG_TEXT);
            Assert.assertTrue(tableSchema.getColumn(1).getAllowNull());
            // FLOAT -> PG_DOUBLE_PRECISION
            Assert.assertEquals(tableSchema.getColumn(2).getName(), "c");
            Assert.assertEquals(tableSchema.getColumn(2).getType(), Types.DOUBLE);
            Assert.assertEquals(
                    tableSchema.getColumn(2).getTypeName(), HologresTypes.PG_DOUBLE_PRECISION);
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            // 2. alter column
            HashMap<String, DataType> dataTypeHashMap = new HashMap<>();
            dataTypeHashMap.put("a", DataTypes.BIGINT());
            dataTypeHashMap.put("b", DataTypes.VARBINARY(2));
            dataTypeHashMap.put("c", DataTypes.DOUBLE());
            AlterColumnTypeEvent alterColumnTypeEvent =
                    new AlterColumnTypeEvent(tableId, dataTypeHashMap);
            applier.applySchemaChange(alterColumnTypeEvent);
            tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(tableSchema.getColumn(0).getName(), "a");
            Assert.assertEquals(tableSchema.getColumn(0).getType(), Types.BIGINT);
            Assert.assertEquals(tableSchema.getColumn(0).getTypeName(), HologresTypes.PG_BIGINT);
            Assert.assertEquals(tableSchema.getColumn(1).getName(), "b");
            Assert.assertEquals(tableSchema.getColumn(1).getType(), Types.VARCHAR);
            Assert.assertEquals(tableSchema.getColumn(1).getTypeName(), HologresTypes.PG_TEXT);
            Assert.assertTrue(tableSchema.getColumn(1).getAllowNull());
            Assert.assertEquals(tableSchema.getColumn(2).getName(), "c");
            Assert.assertEquals(tableSchema.getColumn(2).getType(), Types.DOUBLE);
            Assert.assertEquals(
                    tableSchema.getColumn(2).getTypeName(), HologresTypes.PG_DOUBLE_PRECISION);
            Assert.assertTrue(tableSchema.getColumn(2).getAllowNull());

            // 3. alter column which is not compatibility in TypeNormalization.
            dataTypeHashMap.put("a", DataTypes.STRING());
            applier.applySchemaChange(alterColumnTypeEvent);

            throw new Exception("If throw this exception, mean that job execute successfully.");
        } catch (Exception ex) {
            Assertions.assertThat(ex)
                    .rootCause()
                    .hasMessageContaining("Hologres not support alter columnType now");

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testCreateTableAllTypeWithOnlyBigintOrTextStrategy() {
        Object[][] allTypeMapping =
                new Object[][] {
                    new Object[] {
                        "a", DataTypes.CHAR(10).notNull(), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {"b", DataTypes.VARCHAR(10), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"c", DataTypes.STRING(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"d", DataTypes.BOOLEAN(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"e", DataTypes.BINARY(10), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {
                        "f", DataTypes.VARBINARY(10), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {"g", DataTypes.BYTES(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {
                        "h", DataTypes.DECIMAL(5, 2), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {"i", DataTypes.TINYINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"j", DataTypes.SMALLINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"k", DataTypes.INT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"l", DataTypes.BIGINT(), HologresTypes.PG_BIGINT, Types.BIGINT},
                    new Object[] {"m", DataTypes.FLOAT(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"n", DataTypes.DOUBLE(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"o", DataTypes.DATE(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"p", DataTypes.TIME(5), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {"q", DataTypes.TIMESTAMP(), HologresTypes.PG_TEXT, Types.VARCHAR},
                    new Object[] {
                        "r", DataTypes.TIMESTAMP_TZ(), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {
                        "s", DataTypes.TIMESTAMP_LTZ(), HologresTypes.PG_TEXT, Types.VARCHAR
                    },
                    new Object[] {
                        "boolean_array",
                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "int_array",
                        DataTypes.ARRAY(DataTypes.INT()),
                        HologresTypes.PG_BIGINT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "bigint_array",
                        DataTypes.ARRAY(DataTypes.BIGINT()),
                        HologresTypes.PG_BIGINT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "float_array",
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "double_array",
                        DataTypes.ARRAY(DataTypes.DOUBLE()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "char_array",
                        DataTypes.ARRAY(DataTypes.CHAR(2)),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "varchar_array",
                        DataTypes.ARRAY(DataTypes.VARCHAR(2)),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    },
                    new Object[] {
                        "text_array",
                        DataTypes.ARRAY(DataTypes.STRING()),
                        HologresTypes.PG_TEXT_ARRAY,
                        Types.ARRAY
                    }
                };

        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier =
                    getHologresMetadataApplier(
                            new HashMap<>(), TypeNormalizationStrategy.ONLY_BIGINT_OR_TEXT);

            // 1. create table
            Schema.Builder builder = Schema.newBuilder();
            for (int i = 0; i < allTypeMapping.length; i++) {
                builder.physicalColumn(
                        (String) allTypeMapping[i][0], (DataType) allTypeMapping[i][1]);
            }

            Schema schema = builder.build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            for (int i = 0; i < allTypeMapping.length; i++) {
                Assert.assertEquals(allTypeMapping[i][0], tableSchema.getColumn(i).getName());
                Assert.assertEquals(allTypeMapping[i][2], tableSchema.getColumn(i).getTypeName());
                Assert.assertEquals(allTypeMapping[i][3], tableSchema.getColumn(i).getType());
            }

            // apply change when enable TypeNormalization

        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        } finally {
            dropTable(sinkTable);
        }
    }

    // Each time restart from checkpoint/savepoint, the create table event maybe send in duplicate.
    @Test
    public void testCreateTableDuplicate() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema.getTableName());
            Assert.assertEquals("public", tableSchema.getSchemaName());
            Assert.assertEquals(2, tableSchema.getColumnSchema().length);

            // 2. create table again.
            Schema schema2 =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .physicalColumn("c", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            CreateTableEvent createTableEvent2 = new CreateTableEvent(tableId, schema2);
            applier.applySchemaChange(createTableEvent2);

            TableSchema tableSchema2 = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals(sinkTable, tableSchema2.getTableName());
            Assert.assertEquals("public", tableSchema2.getSchemaName());
            // Ignore later CreateTableEvent.
            Assert.assertEquals(2, tableSchema2.getColumnSchema().length);

        } finally {
            dropTable(sinkTable);
        }
    }

    // Each time restart from checkpoint/savepoint, the add column event maybe send in duplicate.
    @Test
    public void testAddColumnDuplicate() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table and add column
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN().notNull())));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(createTableEvent);
            applier.applySchemaChange(addColumnEvent);
            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals("c", tableSchema.getColumn(2).getName());
            Assert.assertEquals(Types.DOUBLE, tableSchema.getColumn(2).getType());
            Assert.assertEquals("d", tableSchema.getColumn(3).getName());
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());

            // 2. add column again
            applier.applySchemaChange(addColumnEvent);
            TableSchema tableSchema2 = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals("c", tableSchema2.getColumn(2).getName());
            Assert.assertEquals(Types.DOUBLE, tableSchema2.getColumn(2).getType());
            Assert.assertEquals("d", tableSchema2.getColumn(3).getName());
            Assert.assertEquals(Types.BIT, tableSchema2.getColumn(3).getType());

        } finally {
            dropTable(sinkTable);
        }
    }

    // Each time restart from checkpoint/savepoint, the rename column event maybe send in duplicate.
    @Test
    public void testRenameColumnDuplicate() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. create table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // 2. add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("c", DataTypes.DOUBLE())));
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn("d", DataTypes.BOOLEAN())));
            AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
            applier.applySchemaChange(addColumnEvent);

            //  3. rename columns
            Map<String, String> nameMapping = new HashMap<>();
            nameMapping.put("a", "a1");
            nameMapping.put("d", "d1");
            RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
            applier.applySchemaChange(renameColumnEvent);
            TableSchema tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals("a1", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertEquals("d1", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());

            //  3. rename columns again
            Map<String, String> nameMapping2 = new HashMap<>();
            nameMapping.put("a", "a1");
            nameMapping.put("d1", "d2");
            RenameColumnEvent renameColumnEvent2 = new RenameColumnEvent(tableId, nameMapping);
            applier.applySchemaChange(renameColumnEvent2);
            tableSchema = holoClient.getTableSchema(sinkTable, true);
            Assert.assertEquals("a1", tableSchema.getColumn(0).getName());
            Assert.assertEquals(Types.INTEGER, tableSchema.getColumn(0).getType());
            Assert.assertEquals("d2", tableSchema.getColumn(3).getName());
            // PG_BOOLEAN -> Types.BIT
            Assert.assertEquals(Types.BIT, tableSchema.getColumn(3).getType());
            Assert.assertTrue(tableSchema.getColumn(3).getAllowNull());

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testTruncateTable() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. Initialize table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // 2. Push in data
            TableSchema holoSchema = holoClient.getTableSchema(sinkTable, true);

            holoClient.put(
                    Arrays.asList(
                            new Put(holoSchema).setObject("a", 17).setObject("b", "Alice"),
                            new Put(holoSchema).setObject("a", 18).setObject("b", "Bury"),
                            new Put(holoSchema).setObject("a", 19).setObject("b", "Carolle")));
            holoClient.flush();

            // 3. Ensure we've got records written into
            Assert.assertArrayEquals(
                    scanRecords(holoClient, holoSchema).stream()
                            .map(Record::getValues)
                            .map(
                                    e ->
                                            Arrays.stream(e)
                                                    .map(Object::toString)
                                                    .collect(Collectors.joining(" | ")))
                            .sorted()
                            .toArray(),
                    new String[] {"17 | Alice", "18 | Bury", "19 | Carolle"});

            // 4. Check truncate event results
            TruncateTableEvent truncateTableEvent = new TruncateTableEvent(tableId);
            applier.applySchemaChange(truncateTableEvent);
            Assert.assertArrayEquals(
                    scanRecords(holoClient, holoSchema).stream()
                            .map(Record::getValues)
                            .map(
                                    e ->
                                            Arrays.stream(e)
                                                    .map(Object::toString)
                                                    .collect(Collectors.joining(" | ")))
                            .sorted()
                            .toArray(),
                    new String[] {});

            // 5. Test it again to ensure new data correctness after TRUNCATE
            holoClient.put(
                    Arrays.asList(
                            new Put(holoSchema).setObject("a", 20).setObject("b", "Derrida"),
                            new Put(holoSchema).setObject("a", 21).setObject("b", "Eve"),
                            new Put(holoSchema).setObject("a", 22).setObject("b", "Ferry")));
            holoClient.flush();

            // 6. Ensure we've got new records written into
            Assert.assertArrayEquals(
                    scanRecords(holoClient, holoSchema).stream()
                            .map(Record::getValues)
                            .map(
                                    e ->
                                            Arrays.stream(e)
                                                    .map(Object::toString)
                                                    .collect(Collectors.joining(" | ")))
                            .sorted()
                            .toArray(),
                    new String[] {"20 | Derrida", "21 | Eve", "22 | Ferry"});

            // 7. Check truncate event results
            applier.applySchemaChange(truncateTableEvent);
            Assert.assertArrayEquals(
                    scanRecords(holoClient, holoSchema).stream()
                            .map(Record::toString)
                            .sorted()
                            .toArray(),
                    new String[] {});

        } finally {
            dropTable(sinkTable);
        }
    }

    @Test
    public void testDropTable() throws HoloClientException {
        try (HoloClient holoClient = getHoloClient()) {
            HologresMetadataApplier applier = getHologresMetadataApplier();

            // 1. Initialize table
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("a", DataTypes.INT().notNull())
                            .physicalColumn("b", DataTypes.STRING())
                            .primaryKey("a")
                            .build();
            TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            applier.applySchemaChange(createTableEvent);

            // 2. Push in data
            TableSchema holoSchema = holoClient.getTableSchema(sinkTable, true);

            holoClient.put(
                    Arrays.asList(
                            new Put(holoSchema).setObject("a", 17).setObject("b", "Alice"),
                            new Put(holoSchema).setObject("a", 18).setObject("b", "Bury"),
                            new Put(holoSchema).setObject("a", 19).setObject("b", "Carolle")));
            holoClient.flush();

            // 3. Ensure we've got records written into
            Assert.assertArrayEquals(
                    scanRecords(holoClient, holoSchema).stream()
                            .map(Record::getValues)
                            .map(
                                    e ->
                                            Arrays.stream(e)
                                                    .map(Object::toString)
                                                    .collect(Collectors.joining(" | ")))
                            .sorted()
                            .toArray(),
                    new String[] {"17 | Alice", "18 | Bury", "19 | Carolle"});

            // 4. Check drop event results
            DropTableEvent dropTableEvent = new DropTableEvent(tableId);
            applier.applySchemaChange(dropTableEvent);

            Assertions.assertThatThrownBy(
                            () -> System.out.println(holoClient.getTableSchema(sinkTable, true)))
                    .hasRootCauseInstanceOf(SQLException.class)
                    .hasMessageContaining(
                            String.format("can not found table \"public\".\"%s\"", sinkTable));

        } finally {
            dropTable(sinkTable);
        }
    }

    private List<Record> scanRecords(HoloClient client, TableSchema schema)
            throws HoloClientException {
        List<Record> records = new ArrayList<>();
        try (RecordScanner rs = client.scan(Scan.newBuilder(schema).build())) {
            while (rs.next()) {
                records.add(rs.getRecord());
            }
        }
        return records;
    }

    private HologresMetadataApplier getHologresMetadataApplier() {
        return getHologresMetadataApplier(new HashMap<>(), TypeNormalizationStrategy.STANDARD);
    }

    private HologresMetadataApplier getHologresMetadataApplier(
            Map<String, String> tableOptions, TypeNormalizationStrategy typeNormalizationStrategy) {
        HologresConnectionParamBuilder hologresConnectionParamBuilder =
                HologresConnectionParam.builder();
        HologresConnectionParam hologresConnectionParam =
                hologresConnectionParamBuilder
                        .setEndpoint(endpoint)
                        .setDatabase(database)
                        .setUsername(username)
                        .setPassword(password)
                        .setTableOptions(tableOptions)
                        .setTypeNormalizationStrategy(typeNormalizationStrategy)
                        .build();
        return new HologresMetadataApplier(hologresConnectionParam);
    }
}
