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

import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.hologres.utils.TimeFormats.SQL_TIMESTAMP_FORMAT;

/** Converter between internal LocalZonedTimestampData and java format. */
public class LocalZonedTimestampDataConverters {

    private static final Map<String, DataFormatConverter> TYPE_TO_CONVERTER;

    static {
        Map<String, DataFormatConverter> t2C = new HashMap<>();
        t2C.put(Timestamp.class.getName(), TimestampConverter.INSTANCE);
        t2C.put(String.class.getName(), StringConverter.INSTANCE);
        TYPE_TO_CONVERTER = Collections.unmodifiableMap(t2C);
    }

    public static DataFormatConverter getDataFormatConverter(String className) {
        DataFormatConverter converter = TYPE_TO_CONVERTER.get(className);
        if (converter != null) {
            return converter;
        }
        throw new FlinkRuntimeException(
                String.format(
                        "Not support to convert from LocalZonedTimestampData to %s", className));
    }

    /** Converter between internal LocalZonedTimestampData and Timestamp. */
    static class TimestampConverter
            extends DataFormatConverter<LocalZonedTimestampData, Timestamp> {
        public static final TimestampConverter INSTANCE = new TimestampConverter();

        public TimestampConverter() {}

        @Override
        Timestamp toExternalImpl(LocalZonedTimestampData localZonedTimestampData) {
            return Timestamp.from(localZonedTimestampData.toInstant());
        }
    }

    /** Converter between internal LocalZonedTimestampData and String( converted to UTC-0). */
    static class StringConverter extends DataFormatConverter<LocalZonedTimestampData, String> {
        public static final StringConverter INSTANCE = new StringConverter();

        public StringConverter() {}

        @Override
        String toExternalImpl(LocalZonedTimestampData localZonedTimestampData) {
            return ZonedDateTime.ofInstant(localZonedTimestampData.toInstant(), ZoneId.of("UTC"))
                    .format(SQL_TIMESTAMP_FORMAT);
        }
    }
}
