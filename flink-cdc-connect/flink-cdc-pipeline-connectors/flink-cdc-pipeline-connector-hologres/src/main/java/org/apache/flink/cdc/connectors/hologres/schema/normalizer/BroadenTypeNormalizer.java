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

package org.apache.flink.cdc.connectors.hologres.schema.normalizer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.hologres.schema.converter.BroadenHoloColumnConverter;
import org.apache.flink.cdc.connectors.hologres.schema.converter.BroadenPgTypeConverter;

import com.alibaba.hologres.client.model.Column;

/**
 * Convert CDC {@link DataType} to Hologres Type in tolerant type normalize mode.
 *
 * <p>TINYINT、SMALLINT、INT、BIGINT -> PG_BIGINT
 *
 * <p>CHAR、VARCHAR、STRING -> PG_TEXT FLOAT
 *
 * <p>DOUBLE -> PG_DOUBLE_PRECISION
 */
@Internal
public class BroadenTypeNormalizer extends StandardTypeNormalizer {
    private static final long serialVersionUID = 1L;

    public BroadenTypeNormalizer() {
        super();
    }

    @Override
    public String transformToHoloType(DataType dataType, boolean isPrimaryKey) {
        BroadenPgTypeConverter tolerantNormalPgTypeTransformer =
                new BroadenPgTypeConverter(isPrimaryKey);
        return dataType.accept(tolerantNormalPgTypeTransformer);
    }

    @Override
    public Column transformToHoloColumn(DataType dataType, boolean isPrimaryKey) {
        BroadenHoloColumnConverter tolerantHoloColumnTransformer =
                new BroadenHoloColumnConverter(isPrimaryKey);
        return dataType.accept(tolerantHoloColumnTransformer);
    }
}
