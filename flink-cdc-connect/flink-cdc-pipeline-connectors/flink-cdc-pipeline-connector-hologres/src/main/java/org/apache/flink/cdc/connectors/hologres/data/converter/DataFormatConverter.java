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

package org.apache.flink.cdc.connectors.hologres.data.converter;

import java.io.Serializable;

/**
 * Converter between internal data format and java format. Inspired by
 * org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter.
 *
 * @param <Internal> Internal data format.
 * @param <External> External data format.
 */
public abstract class DataFormatConverter<Internal, External> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Convert a internal data format to its external(Java) equivalent while automatically handling
     * nulls.
     */
    public final External toExternal(Internal value) {
        return value == null ? null : toExternalImpl(value);
    }

    /** Convert a non-null internal data format to its external(Java) equivalent. */
    abstract External toExternalImpl(Internal value);
}
