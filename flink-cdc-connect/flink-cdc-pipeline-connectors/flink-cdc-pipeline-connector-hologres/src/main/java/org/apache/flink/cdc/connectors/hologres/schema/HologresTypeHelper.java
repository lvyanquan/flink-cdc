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

package org.apache.flink.cdc.connectors.hologres.schema;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.HologresTypeNormalizer;

import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.hologres.schema.HologresTypes.PG_TIMESTAMP_TIME_PRECISION;

/**
 * A util helper to transform between CDC data types, Hologers/Postgres types, and jdbc Types). CDC
 * DataType | Postgres Type | Jdbc Types PG_CHAR
 */
public class HologresTypeHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HologresTypeHelper.class);

    public static TableSchema inferTableSchema(
            Schema schema,
            TableId tableId,
            @Nullable String shardKeys,
            HologresTypeNormalizer hologresTypeNormalizer) {
        TableSchema.Builder builder = new TableSchema.Builder();
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<Column> columns = schema.getColumns();

        builder.setTableName(TableName.valueOf(tableId.getSchemaName(), tableId.getTableName()));
        builder.setComment(schema.comment());

        builder.setColumns(
                columns.stream()
                        .map(
                                column ->
                                        HologresTypeHelper.inferColumn(
                                                column,
                                                primaryKeys.contains(column.getName()),
                                                hologresTypeNormalizer))
                        .collect(Collectors.toList()));

        if (!partitionKeys.isEmpty()) {
            String partitionKey = partitionKeys.get(0);
            builder.setPartitionColumnName(partitionKey);
        }

        // Set default distribution keys as primary keys to write same pk value to same shard.
        if (StringUtils.isNullOrWhitespaceOnly(shardKeys)) {
            builder.setDistributionKeys(primaryKeys.toArray(new String[0]));
        } else {
            builder.setDistributionKeys(shardKeys.split(","));
        }

        TableSchema tableSchema = builder.build();
        tableSchema.calculateProperties();
        return tableSchema;
    }

    public static com.alibaba.hologres.client.model.Column inferColumn(
            Column column, boolean isPrimaryKey, HologresTypeNormalizer hologresTypeNormalizer) {
        com.alibaba.hologres.client.model.Column holoColumn =
                hologresTypeNormalizer.transformToHoloColumn(column.getType(), isPrimaryKey);
        holoColumn.setName(column.getName());
        holoColumn.setComment(column.getComment());
        return holoColumn;
    }

    public static String toPostgresType(
            DataType dataType,
            boolean isPrimaryKey,
            HologresTypeNormalizer hologresTypeNormalizer) {
        validatePrecision(dataType);
        String jdbcType = hologresTypeNormalizer.transformToHoloType(dataType, isPrimaryKey);
        if (!dataType.isNullable()) {
            return String.format("%s NOT NULL", jdbcType);
        } else {
            return jdbcType;
        }
    }

    public static String toNullablePostgresType(
            DataType dataType,
            boolean isPrimaryKey,
            HologresTypeNormalizer hologresTypeNormalizer) {
        validatePrecision(dataType);
        return hologresTypeNormalizer.transformToHoloType(dataType, isPrimaryKey);
    }

    private static void validatePrecision(DataType dataType) {
        int precision;
        switch (dataType.getTypeRoot()) {
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((TimeType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                precision = ((TimestampType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = ((LocalZonedTimestampType) dataType).getPrecision();
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                precision = ((ZonedTimestampType) dataType).getPrecision();
                break;
            default:
                return;
        }

        if (precision > PG_TIMESTAMP_TIME_PRECISION) {
            LOG.warn(
                    "Hologres only supports the precision is {} for {} but the specified precision is {}. "
                            + "When writing to the hologres with larger precision, the sink will CAST the data with the supported precision. "
                            + "To keep the precision, please CAST the value to STRING type if cares.",
                    PG_TIMESTAMP_TIME_PRECISION,
                    dataType.getTypeRoot(),
                    precision);
        }
    }
}
