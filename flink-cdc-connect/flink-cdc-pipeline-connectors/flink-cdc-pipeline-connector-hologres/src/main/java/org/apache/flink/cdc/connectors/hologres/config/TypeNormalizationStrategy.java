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

import org.apache.flink.cdc.connectors.hologres.schema.normalizer.BroadenTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.HologresTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.OnlyBigintOrTextTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.StandardTypeNormalizer;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * The strategy when hologres sink transform upstream data to holo type.
 *
 * <p>NORMAL: Flink CDC type to PG type conversion according to standard.
 *
 * <p>BROADEN: Flink CDC type converted to a broader Holo type.
 *
 * <p>ONLY_BIGINT_OR_TEXT: ALL Flink CDC types converted to BIGINT or STRING types in Holo.
 */
public enum TypeNormalizationStrategy {
    STANDARD,
    BROADEN,
    ONLY_BIGINT_OR_TEXT;

    public static HologresTypeNormalizer getHologresTypeNormalizer(
            TypeNormalizationStrategy typeNormalizationStrategy) {
        switch (typeNormalizationStrategy) {
            case STANDARD:
                return new StandardTypeNormalizer();
            case BROADEN:
                return new BroadenTypeNormalizer();
            case ONLY_BIGINT_OR_TEXT:
                return new OnlyBigintOrTextTypeNormalizer();
            default:
                throw new FlinkRuntimeException(
                        "Not support typeNormalizationStrategy:" + typeNormalizationStrategy);
        }
    }
}
