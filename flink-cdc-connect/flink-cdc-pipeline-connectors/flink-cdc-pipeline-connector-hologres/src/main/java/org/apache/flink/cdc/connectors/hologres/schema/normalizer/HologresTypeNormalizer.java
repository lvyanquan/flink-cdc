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
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import com.alibaba.hologres.client.model.Column;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * Transform From CDC data types to Hologers/Postgres types or HoloColumn which depends on {@link
 * org.apache.flink.cdc.connectors.hologres.config.TypeNormalizationStrategy}.
 */
@Internal
public interface HologresTypeNormalizer extends Serializable {

    /** Transforms CDC {@link DataType} to Postgres(or hologres ) data type string. */
    String transformToHoloType(DataType dataType, boolean isPrimaryKey);

    /**
     * Transforms CDC {@link DataType} to Hologres {@link com.alibaba.hologres.client.model.Column}.
     *
     * @return
     */
    Column transformToHoloColumn(DataType dataType, boolean isPrimaryKey);

    /**
     * Creates an accessor for getting elements in an internal RecordData structure at the given
     * position and coverter to data formant which hologres record need.
     *
     * @param schema the schema type of the RecordData
     * @return
     */
    RecordData.FieldGetter[] createFieldGetters(Schema schema, ZoneId zoneId);
}
