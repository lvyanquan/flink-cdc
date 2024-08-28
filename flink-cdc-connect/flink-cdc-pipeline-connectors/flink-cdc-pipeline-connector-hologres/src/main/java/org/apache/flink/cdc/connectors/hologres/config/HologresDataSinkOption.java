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

package org.apache.flink.cdc.connectors.hologres.config;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import com.alibaba.hologres.client.model.WriteMode;

/** Configs only used for sink. */
public class HologresDataSinkOption {

    public static final ConfigOption<DeleteStrategy> SINK_DELETE_STRATEGY =
            ConfigOptions.key("sink.delete-strategy".toLowerCase())
                    .enumType(DeleteStrategy.class)
                    .defaultValue(DeleteStrategy.DELETE_ROW_ON_PK)
                    .withDescription(
                            "Whether to ignore delete events. If set to DELETE_ROW_ON_PK, rows will be deleted based on the primary key. If set to IGNORE_DELETE, delete operations will be ignored.");

    public static final ConfigOption<WriteMode> MUTATE_TYPE =
            ConfigOptions.key("mutateType".toLowerCase())
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.INSERT_OR_UPDATE)
                    .withDescription(
                            "Data write mode. If set to insertorignore: retains the first occurrence of data and ignores all subsequent occurrences. If set to insertorreplace: later occurring data will replace the existing data in its entirety. If set to insertorupdate(default value): updates specific columns of the existing data. ");

    public static final ConfigOption<Boolean> IGNORE_NULL_WHEN_UPDATE =
            ConfigOptions.key("ignoreNullWhenUpdate".toLowerCase())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When mutatetype='insertOrUpdate', whether to ignore Null values in the update write data.");

    // JDBC config
    public static final ConfigOption<Boolean> CREATE_MISSING_PARTITION_TABLE =
            ConfigOptions.key("createPartTable".toLowerCase())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When writing to a partitioned table, whether to automatically create non-existing partitions based on the partition values.");

    public static final ConfigOption<Integer> OPTIONAL_JDBC_WRITE_BATCH_SIZE =
            ConfigOptions.key("jdbcWriteBatchSize".toLowerCase())
                    .intType()
                    .defaultValue(256)
                    .withDescription(
                            "When AsyncCommit is set to true, upon calling the put method, if the number of records is greater than or equal to writeBatchSize, or the total number of bytes of records is greater than or equal to writeBatchByteSize, then call flush to perform batch submission.");

    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE =
            ConfigOptions.key("jdbcWriteBatchByteSize".toLowerCase())
                    .longType()
                    .defaultValue(2097152L)
                    .withDescription(
                            "When AsyncCommit is true, upon calling the put method, if the number of records is greater than or equal to writeBatchSize, or the total number of bytes of records is greater than or equal to writeBatchByteSize, then call flush to perform batch submission with a List. ");
    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_BATCH_TOTAL_BYTE_SIZE =
            ConfigOptions.key("jdbcWriteBatchTotalByteSize".toLowerCase())
                    .longType()
                    .defaultValue(20971520L)
                    .withDescription(
                            "The maximum batchSize for batch accumulation across all tables, upon which flush is called to perform batch submission with a List.");
    public static final ConfigOption<Long> OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL =
            ConfigOptions.key("jdbcWriteFlushInterval".toLowerCase())
                    .longType()
                    .defaultValue(10000L)
                    .withDescription(
                            "In JDBC mode, the maximum waiting time for the Hologres Sink node to batch and write data into Hologres.");

    public static final ConfigOption<Boolean> OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN =
            ConfigOptions.key("jdbcEnableDefaultForNotNullColumn".toLowerCase())
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether the connector is allowed to fill in a default value if a Null value is written into a Not Null field without a default value in a Hologres table.");
    public static final ConfigOption<Boolean> OPTIONAL_ENABLE_REMOVE_U0000_IN_TEXT =
            ConfigOptions.key("remove-u0000-in-text.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the connector is allowed to remove \\u0000 illegal characters if the string type contains them during writing.");
    public static final ConfigOption<Boolean> OPTIONAL_ENABLE_DEDUPLICATION =
            ConfigOptions.key("deduplication.enabled".toLowerCase())
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "During the JDBC batching writing, whether deduplication is performed.");

    public static final ConfigOption<Boolean> USE_FIXED_FE =
            ConfigOptions.key("useFixedFe".toLowerCase())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use a fixed frontend for writing and reading");

    public static final ConfigOption<TypeNormalizationStrategy> TYPE_NORMALIZATION_STRATEGY =
            ConfigOptions.key("sink.type-normalize-strategy".toLowerCase())
                    .enumType(TypeNormalizationStrategy.class)
                    .defaultValue(TypeNormalizationStrategy.STANDARD)
                    .withDescription(
                            "Whether to enable type normalization. If type normalization is enabled, TINYINT, SMALLINT, INT, and BIGINT types From CDC DataTypes will be converted to BIGINT type in Hologres; CHAR, VARCHAR, and STRING From CDC DataTypes will be converted to TEXT type in Hologres; FLOAT and DOUBLE From CDC DataTypes will be converted to DOUBLE PRECISION in Hologres.");

    // table property
    public static final String TABLE_PROPERTY_PREFIX = "table_property.";
}
