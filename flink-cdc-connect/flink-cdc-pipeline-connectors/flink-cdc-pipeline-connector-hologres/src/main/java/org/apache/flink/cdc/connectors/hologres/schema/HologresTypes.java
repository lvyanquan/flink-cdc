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

/** Postgres type witch hologres contains. */
public class HologresTypes {
    public static final String PG_SERIAL = "serial";
    public static final String PG_BIGSERIAL = "bigserial";
    public static final String PG_BYTEA = "bytea";
    public static final String PG_BYTEA_ARRAY = "_bytea";
    public static final String PG_ROARING_BITMAP = "roaringbitmap";
    public static final String PG_SMALLINT = "int2";
    public static final String PG_SMALLINT_ARRAY = "_int2";
    public static final String PG_INTEGER = "int4";
    public static final String PG_INTEGER_ARRAY = "_int4";
    public static final String PG_BIGINT = "int8";
    public static final String PG_BIGINT_ARRAY = "_int8";
    public static final String PG_REAL = "float4";
    public static final String PG_REAL_ARRAY = "_float4";
    public static final String PG_DOUBLE_PRECISION = "float8";
    public static final String PG_DOUBLE_PRECISION_ARRAY = "_float8";
    public static final String PG_NUMERIC = "numeric";
    public static final String PG_NUMERIC_ARRAY = "_numeric";
    public static final String PG_BOOLEAN = "bool";
    public static final String PG_BOOLEAN_ARRAY = "_bool";
    public static final String PG_TIMESTAMP = "timestamp";
    public static final String PG_TIMESTAMP_ARRAY = "_timestamp";
    public static final String PG_TIMESTAMPTZ = "timestamptz";
    public static final String PG_TIMESTAMPTZ_ARRAY = "_timestamptz";
    public static final String PG_DATE = "date";
    public static final String PG_DATE_ARRAY = "_date";
    public static final String PG_TIME = "time";
    public static final String PG_TIME_ARRAY = "_time";
    public static final String PG_TEXT = "text";
    public static final String PG_TEXT_ARRAY = "_text";
    public static final String PG_CHAR = "bpchar";
    public static final String PG_CHAR_ARRAY = "_bpchar";
    public static final String PG_CHARACTER = "character";
    public static final String PG_CHARACTER_ARRAY = "_character";
    public static final String PG_CHARACTER_VARYING = "varchar";
    public static final String PG_CHARACTER_VARYING_ARRAY = "_varchar";
    public static final String PG_JSON = "json";
    public static final String PG_JSONB = "jsonb";
    public static final String PG_GEOGRAPHY = "geography";
    public static final String PG_GEOMETRY = "geometry";
    public static final String PG_BOX2D = "box2d";
    public static final String PG_BOX3D = "box3d";
    public static final int PG_VARCHAR_MAX_SIZE = 10485760;
    public static final int PG_TIMESTAMP_TIME_PRECISION = 6;
}
