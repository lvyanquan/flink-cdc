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

import org.apache.flink.cdc.connectors.hologres.schema.normalizer.HologresTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.NormalTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.StringOrBigintTypeNormalizer;
import org.apache.flink.cdc.connectors.hologres.schema.normalizer.TolerantTypeNormalizer;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * The strategy when hologres sink transform upstream data to holo type. If using NORMAL, each type
 * is mapping to corresponding type in hologres. If using TOLERANCE, each type is mapping to
 * corresponding widest type in hologres. If using TOLERANCE, tiny int, smallint, int, long and
 * bigint are mapping to bigint in hologres, while others are mapping to string in hologres.
 */
public enum TypeNormalizationStrategy {
    NORMAL,
    TOLERANCE,
    STRING_OR_BIGINT;

    public static HologresTypeNormalizer getHologresTypeNormalizer(
            TypeNormalizationStrategy typeNormalizationStrategy) {
        switch (typeNormalizationStrategy) {
            case NORMAL:
                return new NormalTypeNormalizer();
            case TOLERANCE:
                return new TolerantTypeNormalizer();
            case STRING_OR_BIGINT:
                return new StringOrBigintTypeNormalizer();
            default:
                throw new FlinkRuntimeException(
                        "Not support typeNormalizationStrategy:" + typeNormalizationStrategy);
        }
    }
}
