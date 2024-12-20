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

package org.apache.flink.cdc.connectors.hologres.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.hologres.HologresTestBase;
import org.apache.flink.cdc.connectors.hologres.HologresTestUtils;
import org.apache.flink.cdc.connectors.hologres.config.DeleteStrategy;
import org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy;
import org.apache.flink.cdc.connectors.hologres.sink.HologresRecordEventSerializer;
import org.apache.flink.cdc.connectors.hologres.sink.v2.HologresRecordSerializer;
import org.apache.flink.cdc.connectors.hologres.sink.v2.HologresSink;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParam;
import org.apache.flink.cdc.connectors.hologres.sink.v2.config.HologresConnectionParamBuilder;
import org.apache.flink.cdc.connectors.hologres.utils.JDBCUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.CREATE_MISSING_PARTITION_TABLE;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.SINK_DELETE_STRATEGY;
import static org.apache.flink.cdc.connectors.hologres.config.HologresDataSinkOption.TYPE_NORMALIZATION_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for holo sink. */
public class HologresSinkITCase extends HologresTestBase {

    @BeforeEach
    public void setup() throws Exception {
        this.sinkTable = "test_sink_table_" + randomSuffix;
        executeBatchSql("sink_table_ddl.sql", "public", this.sinkTable, false);
    }

    @AfterEach
    public void cleanup() {
        dropTable(sinkTable);
    }

    @Test
    public void testNullValues() throws Exception {
        TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);

        String[] fieldNames =
                new String[] {
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"
                };
        String[] pkFieldNames = new String[] {"h"};

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.CHAR(1),
                    DataTypes.VARCHAR(20),
                    DataTypes.STRING().notNull(),
                    DataTypes.BOOLEAN(),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_TZ(6),
                    DataTypes.TIMESTAMP_LTZ()
                };

        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        null, null, null, null, null, null, null, 0, null, null, null, null, null,
                        null, null, null,
                    }
                };

        String[] expected =
                new String[] {
                    "null,null,null,null,null,null,null,0,null,null,null,null,null,null,null,null"
                };

        testaInsertSingleTable(
                tableId, fieldNames, pkFieldNames, dataTypes, insertedValues, expected);
    }

    @Test
    public void testAllSingleType() throws Exception {

        TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);

        String[] fieldNames =
                new String[] {
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"
                };
        String[] pkFieldNames = new String[] {"h"};

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.CHAR(1),
                    DataTypes.VARCHAR(20),
                    DataTypes.STRING().notNull(),
                    DataTypes.BOOLEAN(),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_TZ(6),
                    DataTypes.TIMESTAMP_LTZ()
                };

        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        BinaryStringData.fromString("a"),
                        BinaryStringData.fromString("test character"),
                        BinaryStringData.fromString("test text"),
                        false,
                        DecimalData.fromBigDecimal(new BigDecimal("8119.21"), 6, 2),
                        Byte.valueOf("1"),
                        Short.valueOf("32767"),
                        32768,
                        652482L,
                        20.2007F,
                        8.58965,
                        //  2023-11-12 - 1970-01-01 = 19673 days
                        19673,
                        (8 * 3600 + 30 * 60 + 15) * 1000,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)),
                        ZonedTimestampData.fromZonedDateTime(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                        .atZone(ZoneId.of("GMT+05:00"))),
                        LocalZonedTimestampData.fromInstant(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                        .atZone(ZoneId.of("GMT+05:00"))
                                        .toInstant())
                    }
                };
        // default timezone is asian/shanghai
        String[] expected =
                new String[] {
                    "a,test character,test text,false,8119.21,1,32767,32768,652482,20.2007,8.58965,2023-11-12,08:30:15,2023-11-11 11:11:11.0,2023-11-11 14:11:11.0,2023-11-11 14:11:11.0"
                };

        testaInsertSingleTable(
                tableId, fieldNames, pkFieldNames, dataTypes, insertedValues, expected);
    }

    // not support array now
    //    @Test
    //    public void testArrayData() throws Exception{
    //        TableId tableId = TableId.tableId("default_namespace", "public", sinkTable);
    //
    //        String[] fieldNames =
    //                new String[] {
    //                        "h",
    //                        "q",
    //                        "r",
    //                        "s",
    //                        "t",
    //                        "u",
    //                        "v",
    //                };
    //        String[] pkFieldNames = new String[] {"h"};
    //
    //        DataType[] dataTypes =
    //                new DataType[] {
    //                        DataTypes.INT(),
    //                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
    //                        DataTypes.ARRAY(DataTypes.INT()),
    //                        DataTypes.ARRAY(DataTypes.BIGINT()),
    //                        DataTypes.ARRAY(DataTypes.FLOAT()),
    //                        DataTypes.ARRAY(DataTypes.DOUBLE()),
    //                        DataTypes.ARRAY(DataTypes.VARCHAR(2)),
    //                        DataTypes.ARRAY(DataTypes.VARCHAR(5))
    //                };
    //
    //        Object[][] insertedValues =
    //                new Object[][] {
    //                        new Object[] {
    //                               1,
    //                                new GenericArrayData(new int[]{1, 2, 3}),
    //                                new GenericArrayData(new long[]{652482L,  -10000L}),
    //                                new GenericArrayData(new float[]{1.22F,20.2007F }),
    //                                new GenericArrayData(new double[]{8.58965, 0}),
    //                                new GenericArrayData(new
    // BinaryStringData[]{BinaryStringData.fromString("ab")}),
    //                                new GenericArrayData(new
    // BinaryStringData[]{BinaryStringData.fromString("a"),BinaryStringData.fromString("abcde")})
    //
    //                        }
    //                };
    //        // default timezone is asian/shanghai
    //        String[] expected =
    //                new String[] {
    //                        "1,[652482,10000],[1.22,20.2007],[8.58965,0],[ab],[a,abcde]"
    //                };
    //
    //        try {
    //            submitJob(tableId, fieldNames, pkFieldNames, dataTypes, insertedValues, expected);
    //        } catch (HoloClientException e) {
    //            throw new RuntimeException(e);
    //        } finally {
    //            dropTable(sinkTable);
    //        }
    //    }

    @Test
    public void testPartitionTable() throws Exception {
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + "id int, "
                        + "ds text, "
                        + "title text, "
                        + "body text, "
                        + "primary key(id, ds)) "
                        + "partition by list(ds);";

        String partitionSinkTable = "partition_" + sinkTable;

        TableId tableId = TableId.tableId("default_namespace", "public", partitionSinkTable);

        String[] fieldNames = new String[] {"id", "ds", "title", "body"};

        String[] pkFieldNames = new String[] {"id", "ds"};
        String[] partitionKeys = {"ds"};
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                };
        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        1,
                        BinaryStringData.fromString("20220727"),
                        BinaryStringData.fromString("title_01"),
                        BinaryStringData.fromString("partition_test")
                    }
                };
        String[] expected = new String[] {"1,20220727,title_01,partition_test"};

        try {
            createTable(prepareCreateTableSql.replace("TABLE_NAME", partitionSinkTable));
            testaInsertSingleTable(
                    tableId,
                    fieldNames,
                    pkFieldNames,
                    dataTypes,
                    insertedValues,
                    expected,
                    true,
                    DeleteStrategy.DELETE_ROW_ON_PK,
                    TypeNormalizationStrategy.STANDARD,
                    partitionKeys,
                    ZoneId.systemDefault());
        } finally {
            dropTable(partitionSinkTable);
        }
    }

    @Test
    public void testDecimalAsPk() throws Exception {
        String decimalTable = "partition_" + sinkTable;
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + " decimal_pk1 character varying, "
                        + " decimal_pk2 character varying, "
                        + "primary key(decimal_pk1, decimal_pk2)); ";

        TableId tableId = TableId.tableId("default_namespace", "public", decimalTable);
        createTable(prepareCreateTableSql.replace("TABLE_NAME", decimalTable));

        String[] fieldNames =
                new String[] {
                    "decimal_pk1", "decimal_pk2",
                };
        String[] pkFieldNames = new String[] {"decimal_pk1", "decimal_pk2"};
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.DECIMAL(20, 0), DataTypes.DECIMAL(19, 17),
                };
        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        DecimalData.fromBigDecimal(
                                new BigDecimal(Long.valueOf(Integer.MIN_VALUE).longValue() * 2),
                                20,
                                0),
                        DecimalData.fromBigDecimal(new BigDecimal("20.0"), 19, 17),
                    },
                    new Object[] {
                        DecimalData.fromBigDecimal(
                                new BigDecimal(Long.valueOf(Integer.MAX_VALUE).longValue() * 2),
                                20,
                                0),
                        DecimalData.fromBigDecimal(new BigDecimal("20.00000000000000001"), 19, 17),
                    }
                };
        String[] expected =
                new String[] {
                    "-4294967296,20.00000000000000000", "4294967294,20.00000000000000001"
                };

        try {
            testaInsertSingleTable(
                    tableId, fieldNames, pkFieldNames, dataTypes, insertedValues, expected);
        } finally {
            dropTable(decimalTable);
        }
    }

    @Test
    public void testDeleteWithoutPK() {

        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT().notNull())
                        .physicalColumn("c", DataTypes.STRING())
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        try {
            submitJob(events);

        } catch (Exception ex) {
            assertThat(ex)
                    .cause()
                    .cause()
                    .cause()
                    .hasMessageContaining("Delete Put table must have primary key");
        }
    }

    @Test
    public void testDelete() throws Exception {

        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT().notNull())
                        .physicalColumn("c", DataTypes.STRING())
                        .primaryKey("h")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        submitJob(events);
        String[] expected = new String[] {"2,Bob"};
        String[] fieldNames = {"h", "c"};
        HologresTestUtils.checkResultWithTimeout(
                expected,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testDeleteWithDeletionAction() throws Exception {

        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT().notNull())
                        .physicalColumn("c", DataTypes.STRING())
                        .primaryKey("h")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        submitJob(
                events,
                false,
                DeleteStrategy.IGNORE_DELETE,
                TypeNormalizationStrategy.STANDARD,
                ZoneId.systemDefault());
        String[] expected = new String[] {"1,Alice", "2,Bob"};
        String[] fieldNames = {"h", "c"};
        HologresTestUtils.checkResultWithTimeout(
                expected,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testUpdate() throws Exception {

        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT())
                        .physicalColumn("c", DataTypes.STRING())
                        .primaryKey("h")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));

        events.add(
                DataChangeEvent.updateEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")}),
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Mike")})));

        submitJob(events);

        String[] expected = new String[] {"1,Mike", "2,Bob"};
        String[] fieldNames = {"h", "c"};
        HologresTestUtils.checkResultWithTimeout(
                expected,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testInsertWithTolerateInBroadenStrategy() throws Exception {
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + "tiny_int_type bigint, "
                        + "small_int_type bigint, "
                        + "int_type bigint, "
                        + "big_type bigint, "
                        + "char_type text, "
                        + "varchar_type text, "
                        + "string_type text, "
                        + "float_type float8, "
                        + "double_type float8, "
                        + "primary key(tiny_int_type)); ";
        String typeNormalizationTable = "tolerate_1_" + sinkTable;

        String[] fieldNames =
                new String[] {
                    "tiny_int_type",
                    "small_int_type",
                    "int_type",
                    "big_type",
                    "char_type",
                    "varchar_type",
                    "string_type",
                    "float_type",
                    "double_type"
                };

        String[] pkFieldNames = new String[] {"tiny_int_type"};
        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.TINYINT().notNull(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.CHAR(5),
                    DataTypes.VARCHAR(100),
                    DataTypes.STRING(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE()
                };
        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        Byte.valueOf("1"),
                        Short.valueOf("32767"),
                        32768,
                        652482L,
                        BinaryStringData.fromString("a"),
                        BinaryStringData.fromString("test character"),
                        BinaryStringData.fromString("test text"),
                        20.125F,
                        8.58965,
                    },
                    new Object[] {
                        Byte.valueOf("2"), null, null, null, null, null, null, null, null,
                    }
                };
        String[] expected =
                new String[] {
                    "1,32767,32768,652482,a,test character,test text,20.125,8.58965",
                    "2,null,null,null,null,null,null,null,null"
                };
        TableId tableId = TableId.tableId("default_namespace", "test", typeNormalizationTable);

        try {
            createTable(
                    prepareCreateTableSql.replace("TABLE_NAME", "test." + typeNormalizationTable));
            testaInsertSingleTable(
                    tableId,
                    fieldNames,
                    pkFieldNames,
                    dataTypes,
                    insertedValues,
                    expected,
                    false,
                    DeleteStrategy.DELETE_ROW_ON_PK,
                    TypeNormalizationStrategy.BROADEN,
                    new String[0],
                    ZoneId.systemDefault());
        } finally {
            dropTable("test." + typeNormalizationTable);
        }
    }

    @Test
    public void testUpdateWithTolerateWithBroadenStrategy() throws Exception {
        String typeNormalizationTable = "tolerate_2_" + sinkTable;
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + "a bigint, "
                        + "b text, "
                        + "c float8, "
                        + "primary key(a)); ";

        TableId myTable1 = TableId.tableId("default_namespace", "public", typeNormalizationTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("a", DataTypes.TINYINT())
                        .physicalColumn("b", DataTypes.CHAR(2))
                        .physicalColumn("c", DataTypes.FLOAT())
                        .primaryKey("a")
                        .build();
        // 1. create table then insert
        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {
                                    Byte.valueOf("1"), BinaryStringData.fromString("a"), 20.125F
                                })));
        // 2. alter column then insert
        HashMap<String, DataType> alterMap1 = new HashMap<>();
        alterMap1.put("a", DataTypes.SMALLINT());
        alterMap1.put("b", DataTypes.VARCHAR(15));
        alterMap1.put("c", DataTypes.DOUBLE());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap1));

        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.SMALLINT(),
                                            DataTypes.VARCHAR(15),
                                            DataTypes.DOUBLE()
                                        })
                                .generate(
                                        new Object[] {
                                            Short.valueOf("32767"),
                                            BinaryStringData.fromString("test character"),
                                            8.58965
                                        })));
        // 3. alter column then insert
        HashMap<String, DataType> alterMap2 = new HashMap<>();
        alterMap2.put("a", DataTypes.INT());
        alterMap2.put("b", DataTypes.VARCHAR(10));
        alterMap2.put("c", DataTypes.DOUBLE());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap2));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.INT(),
                                            DataTypes.VARCHAR(10),
                                            DataTypes.DOUBLE()
                                        })
                                .generate(
                                        new Object[] {
                                            32768, BinaryStringData.fromString("test char"), 8.58965
                                        })));

        // 4. alter column then insert
        HashMap<String, DataType> alterMap3 = new HashMap<>();
        alterMap3.put("a", DataTypes.BIGINT());
        alterMap3.put("b", DataTypes.STRING());
        alterMap3.put("c", DataTypes.FLOAT());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap3));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING(),
                                            DataTypes.FLOAT()
                                        })
                                .generate(
                                        new Object[] {
                                            652482L,
                                            BinaryStringData.fromString("test string"),
                                            20.125F
                                        })));
        try {
            createTable(prepareCreateTableSql.replace("TABLE_NAME", typeNormalizationTable));
            submitJob(
                    events,
                    false,
                    DeleteStrategy.DELETE_ROW_ON_PK,
                    TypeNormalizationStrategy.BROADEN,
                    ZoneId.systemDefault());

            String[] expected =
                    new String[] {
                        "1,a,20.125",
                        "32767,test character,8.58965",
                        "32768,test char,8.58965",
                        "652482,test string,20.125"
                    };
            String[] fieldNames = {"a", "b", "c"};
            HologresTestUtils.checkResultWithTimeout(
                    expected,
                    typeNormalizationTable,
                    fieldNames,
                    JDBCUtils.getDbUrl(endpoint, database),
                    username,
                    password,
                    10000);
        } finally {
            dropTable(typeNormalizationTable);
        }
    }

    @Test
    public void testInsertWithOnlyBigIntOrTextStrategy() throws Exception {
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME(\n"
                        + "    a text,\n"
                        + "    b text,\n"
                        + "    c text,\n"
                        + "    d text,\n"
                        + "    e text,\n"
                        + "    f bigint,\n"
                        + "    g bigint,\n"
                        + "    h bigint NOT NULL,\n"
                        + "    i bigint,\n"
                        + "    j text,\n"
                        + "    k text,\n"
                        + "    l text,\n"
                        + "    m text,\n"
                        + "    n text,\n"
                        + "    o text,\n"
                        + "    p text,\n"
                        + "    q text[],\n"
                        + "    r bigint[],\n"
                        + "    s bigint[],\n"
                        + "    t text[],\n"
                        + "    u text[],\n"
                        + "    v text[],\n"
                        + "    w text[],\n"
                        + "   PRIMARY KEY (h)\n"
                        + ");";

        String typeNormalizationTable = "string_or_bigint_1_" + sinkTable;

        String[] fieldNames =
                new String[] {
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"
                };
        String[] pkFieldNames = new String[] {"h"};

        DataType[] dataTypes =
                new DataType[] {
                    DataTypes.CHAR(1),
                    DataTypes.VARCHAR(20),
                    DataTypes.STRING().notNull(),
                    DataTypes.BOOLEAN(),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DATE(),
                    DataTypes.TIME(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP_TZ(6),
                    DataTypes.TIMESTAMP_LTZ()
                };

        Object[][] insertedValues =
                new Object[][] {
                    new Object[] {
                        BinaryStringData.fromString("a"),
                        BinaryStringData.fromString("test character"),
                        BinaryStringData.fromString("test text"),
                        false,
                        DecimalData.fromBigDecimal(new BigDecimal("8119.21"), 6, 2),
                        Byte.valueOf("1"),
                        Short.valueOf("32767"),
                        32768,
                        652482L,
                        20.2007F,
                        8.58965,
                        //  2023-11-12 - 1970-01-01 = 19673 days
                        19673,
                        (8 * 3600 + 30 * 60 + 15) * 1000,
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)),
                        ZonedTimestampData.fromZonedDateTime(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                        .atZone(ZoneId.of("+05:00"))),
                        LocalZonedTimestampData.fromInstant(
                                LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                        .atZone(ZoneId.of("+05:00"))
                                        .toInstant())
                    },
                    {
                        null, null, null, null, null, null, null, 1, null, null, null, null, null,
                        null, null, null
                    }
                };
        // default timezone is asian/shanghai and pipeline time-zone is GMT+06:00
        String[] expected =
                new String[] {
                    "a,test character,test text,false,8119.21,1,32767,32768,652482,20.2007,8.58965,2023-11-12,08:30:15,2023-11-11 11:11:11.000000011,2023-11-11 11:11:11.000000011+05:00,2023-11-11 06:11:11.000000011",
                    "null,null,null,null,null,null,null,1,null,null,null,null,null,null,null,null"
                };
        TableId tableId = TableId.tableId("default_namespace", "test", typeNormalizationTable);

        try {
            createTable(
                    prepareCreateTableSql.replace("TABLE_NAME", "test." + typeNormalizationTable));
            testaInsertSingleTable(
                    tableId,
                    fieldNames,
                    pkFieldNames,
                    dataTypes,
                    insertedValues,
                    expected,
                    false,
                    DeleteStrategy.DELETE_ROW_ON_PK,
                    TypeNormalizationStrategy.ONLY_BIGINT_OR_TEXT,
                    new String[0],
                    ZoneId.of("+06:00"));
        } finally {
            dropTable("test." + typeNormalizationTable);
        }
    }

    @Test
    public void testUpdateWithOnlyBigIntOrTextStrategy() throws Exception {
        String typeNormalizationTable = "string_or_bigint_2_" + sinkTable;
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + "a bigint, "
                        + "b text, "
                        + "c text, "
                        + "primary key(a)); ";

        TableId myTable1 = TableId.tableId("default_namespace", "public", typeNormalizationTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("a", DataTypes.TINYINT())
                        .physicalColumn("b", DataTypes.CHAR(2))
                        .physicalColumn("c", DataTypes.FLOAT())
                        .primaryKey("a")
                        .build();
        // 1. create table then insert
        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {
                                    Byte.valueOf("1"), BinaryStringData.fromString("a"), 20.125F
                                })));
        // 2. alter column then insert
        HashMap<String, DataType> alterMap1 = new HashMap<>();
        alterMap1.put("a", DataTypes.SMALLINT());
        alterMap1.put("b", DataTypes.VARCHAR(15));
        alterMap1.put("c", DataTypes.DOUBLE());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap1));

        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.SMALLINT(),
                                            DataTypes.VARCHAR(15),
                                            DataTypes.DOUBLE()
                                        })
                                .generate(
                                        new Object[] {
                                            Short.valueOf("32767"),
                                            BinaryStringData.fromString("test character"),
                                            8.58965
                                        })));
        // 3. alter column then insert
        HashMap<String, DataType> alterMap2 = new HashMap<>();
        alterMap2.put("a", DataTypes.INT());
        alterMap2.put("b", DataTypes.VARCHAR(10));
        alterMap2.put("c", DataTypes.DOUBLE());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap2));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.INT(),
                                            DataTypes.VARCHAR(10),
                                            DataTypes.DOUBLE()
                                        })
                                .generate(
                                        new Object[] {
                                            32768, BinaryStringData.fromString("test char"), 8.58965
                                        })));

        // 4. alter column then insert
        HashMap<String, DataType> alterMap3 = new HashMap<>();
        alterMap3.put("a", DataTypes.BIGINT());
        alterMap3.put("b", DataTypes.STRING());
        alterMap3.put("c", DataTypes.FLOAT());
        events.add(new AlterColumnTypeEvent(myTable1, alterMap3));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING(),
                                            DataTypes.FLOAT()
                                        })
                                .generate(
                                        new Object[] {
                                            652482L,
                                            BinaryStringData.fromString("test string"),
                                            20.125F
                                        })));
        try {
            createTable(prepareCreateTableSql.replace("TABLE_NAME", typeNormalizationTable));
            submitJob(
                    events,
                    false,
                    DeleteStrategy.DELETE_ROW_ON_PK,
                    TypeNormalizationStrategy.BROADEN,
                    ZoneId.systemDefault());

            String[] expected =
                    new String[] {
                        "1,a,20.125",
                        "32767,test character,8.58965",
                        "32768,test char,8.58965",
                        "652482,test string,20.125"
                    };
            String[] fieldNames = {"a", "b", "c"};
            HologresTestUtils.checkResultWithTimeout(
                    expected,
                    typeNormalizationTable,
                    fieldNames,
                    JDBCUtils.getDbUrl(endpoint, database),
                    username,
                    password,
                    10000);
        } finally {
            dropTable(typeNormalizationTable);
        }
    }

    /**
     * Holo sink will add column as nullable no mather whether AddColumn is NOT NULL. Thus, schema
     * will be different with schema in registry.
     *
     * @throws Exception
     */
    @Test
    public void testInsertAddNotNullColumn() throws Exception {
        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT().notNull())
                        .physicalColumn("c", DataTypes.STRING())
                        .primaryKey("h")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        // The real ddl of column d is nullable.
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("d", DataTypes.BOOLEAN().notNull())));
        AddColumnEvent addColumnEvent = new AddColumnEvent(myTable1, addedColumns);
        events.add(addColumnEvent);
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.INT().notNull(),
                                            DataTypes.STRING(),
                                            DataTypes.BOOLEAN()
                                        })
                                .generate(
                                        new Object[] {
                                            3, BinaryStringData.fromString("Mike"), false
                                        })));

        submitJob(events);
        String[] expected = new String[] {"2,Bob,null", "3,Mike,false"};
        String[] fieldNames = {"h", "c", "d"};
        HologresTestUtils.checkResultWithTimeout(
                expected,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    /**
     * Holo sink will add column as last position no mather what position from AddColumn is. Thus,
     * schema will be different with schema in registry.
     *
     * @throws Exception
     */
    @Test
    public void testInsertAddColumnWithPostition() throws Exception {

        TableId myTable1 = TableId.tableId("default_namespace", "public", sinkTable);
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("h", DataTypes.INT().notNull())
                        .physicalColumn("c", DataTypes.STRING())
                        .primaryKey("h")
                        .build();

        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob")})));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice")})));

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn("d", DataTypes.BOOLEAN()),
                        AddColumnEvent.ColumnPosition.FIRST,
                        null));
        AddColumnEvent addColumnEvent = new AddColumnEvent(myTable1, addedColumns);
        events.add(addColumnEvent);
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BOOLEAN(),
                                            DataTypes.INT().notNull(),
                                            DataTypes.STRING(),
                                        })
                                .generate(
                                        new Object[] {
                                            false, 3, BinaryStringData.fromString("Mike")
                                        })));

        submitJob(events);
        String[] expected = new String[] {"2,Bob,null", "3,Mike,false"};
        String[] fieldNames = {"h", "c", "d"};
        HologresTestUtils.checkResultWithTimeout(
                expected,
                sinkTable,
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    @Test
    public void testMultipleTables() throws Exception {
        TableId tableId1 = TableId.tableId("default_namespace", "public", "multi_1_" + sinkTable);
        TableId tableId2 = TableId.tableId("default_namespace", "public", "multi_2_" + sinkTable);
        String prepareCreateTableSql =
                "CREATE TABLE TABLE_NAME( "
                        + "id int, "
                        + "ds text, "
                        + "title text, "
                        + "body text, "
                        + "primary key(id, ds)); ";

        try {
            createTable(prepareCreateTableSql.replace("TABLE_NAME", tableId1.getTableName()));
            createTable(prepareCreateTableSql.replace("TABLE_NAME", tableId2.getTableName()));
            String[] fieldNames = new String[] {"id", "ds", "title", "body"};

            String[] pkFieldNames = new String[] {"id", "ds"};
            DataType[] dataTypes =
                    new DataType[] {
                        DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                    };
            Object[][] insertedValues =
                    new Object[][] {
                        new Object[] {
                            1,
                            BinaryStringData.fromString("20220727"),
                            BinaryStringData.fromString("title_01"),
                            BinaryStringData.fromString("partition_test")
                        }
                    };
            String[] expected = new String[] {"1,20220727,title_01,partition_test"};

            List<Event> events = new ArrayList<>();
            events.addAll(
                    constructInsertEvents(
                            tableId1,
                            fieldNames,
                            pkFieldNames,
                            dataTypes,
                            insertedValues,
                            new String[0]));
            events.addAll(
                    constructInsertEvents(
                            tableId2,
                            fieldNames,
                            pkFieldNames,
                            dataTypes,
                            insertedValues,
                            new String[0]));

            submitJob(events);

            // check table1 and table2
            HologresTestUtils.checkResultWithTimeout(
                    expected,
                    tableId1.getSchemaName() + "." + tableId1.getTableName(),
                    fieldNames,
                    JDBCUtils.getDbUrl(endpoint, database),
                    username,
                    password,
                    10000);
            HologresTestUtils.checkResultWithTimeout(
                    expected,
                    tableId2.getSchemaName() + "." + tableId2.getTableName(),
                    fieldNames,
                    JDBCUtils.getDbUrl(endpoint, database),
                    username,
                    password,
                    10000);

        } finally {
            dropTable(tableId1.getTableName());
            dropTable(tableId2.getTableName());
        }
    }

    private void testaInsertSingleTable(
            TableId tableId,
            String[] fieldNames,
            String[] pkFieldNames,
            DataType[] dataTypes,
            Object[][] insertedValues,
            String[] expected)
            throws Exception {
        testaInsertSingleTable(
                tableId,
                fieldNames,
                pkFieldNames,
                dataTypes,
                insertedValues,
                expected,
                CREATE_MISSING_PARTITION_TABLE.defaultValue(),
                SINK_DELETE_STRATEGY.defaultValue(),
                TYPE_NORMALIZATION_STRATEGY.defaultValue(),
                new String[0],
                ZoneId.systemDefault());
    }

    private void testaInsertSingleTable(
            TableId tableId,
            String[] fieldNames,
            String[] pkFieldNames,
            DataType[] dataTypes,
            Object[][] insertedValues,
            String[] expected,
            boolean createPartitionTable,
            DeleteStrategy deleteStrategy,
            TypeNormalizationStrategy enableTypeNormalization,
            String[] partitionKeys,
            ZoneId zoneId)
            throws Exception {

        List<Event> events =
                constructInsertEvents(
                        tableId,
                        fieldNames,
                        pkFieldNames,
                        dataTypes,
                        insertedValues,
                        partitionKeys);
        submitJob(events, createPartitionTable, deleteStrategy, enableTypeNormalization, zoneId);
        HologresTestUtils.checkResultWithTimeout(
                expected,
                tableId.getSchemaName() + "." + tableId.getTableName(),
                fieldNames,
                JDBCUtils.getDbUrl(endpoint, database),
                username,
                password,
                10000);
    }

    private List<Event> constructInsertEvents(
            TableId tableId,
            String[] fieldNames,
            String[] pkFieldNames,
            DataType[] dataTypes,
            Object[][] insertedValues,
            String[] partitionKeys) {
        List<Event> events = new ArrayList<>();

        // 1. create table
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < fieldNames.length; i++) {
            builder.physicalColumn(fieldNames[i], dataTypes[i]);
        }

        Schema table1Schema = builder.primaryKey(pkFieldNames).partitionKey(partitionKeys).build();
        events.add(new CreateTableEvent(tableId, table1Schema));

        // table event
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));

        for (int i = 0; i < insertedValues.length; i++) {
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, table1dataGenerator.generate(insertedValues[i])));
        }

        return events;
    }

    private void submitJob(List<Event> events) throws Exception {
        submitJob(
                events,
                CREATE_MISSING_PARTITION_TABLE.defaultValue(),
                SINK_DELETE_STRATEGY.defaultValue(),
                TYPE_NORMALIZATION_STRATEGY.defaultValue(),
                ZoneId.systemDefault());
    }

    private void submitJob(
            List<Event> events,
            boolean createPartitionTable,
            DeleteStrategy deleteStrategy,
            TypeNormalizationStrategy typeNormalizationStrategy,
            ZoneId zoneId)
            throws Exception {

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStreamSource<Event> source =
                environment.fromCollection(events, TypeInformation.of(Event.class));
        HologresConnectionParamBuilder hologresConnectionParamBuilder =
                HologresConnectionParam.builder();
        HologresConnectionParam hologresConnectionParam =
                hologresConnectionParamBuilder
                        .setEndpoint(endpoint)
                        .setDatabase(database)
                        .setUsername(username)
                        .setPassword(password)
                        .setCreatePartitionTable(createPartitionTable)
                        .setDeleteStrategy(deleteStrategy)
                        .setTypeNormalizationStrategy(typeNormalizationStrategy)
                        .setZoneId(zoneId)
                        .build();

        HologresRecordSerializer<Event> serializer =
                new HologresRecordEventSerializer(hologresConnectionParam);
        HologresSink<Event> hologresSink = new HologresSink(hologresConnectionParam, serializer);
        source.sinkTo(hologresSink);
        environment.execute();
    }
}
