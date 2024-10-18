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

package org.apache.flink.cdc.connectors.kafka.source.schema;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.List;

/** Represent the result whether current schema is compatible to accept record of new schema. */
public class SchemaEvolveResult {

    /** Enum for the type of the compatibility. */
    public enum Type {

        /** This indicates that the current schema continued to be used as is. */
        COMPATIBLE_AS_IS,

        /** This indicates that it is possible to be compatible after a schema evolution. */
        COMPATIBLE_AFTER_EVOLUTION,

        /** This indicates that the new schema is incompatible, even with evolution. */
        INCOMPATIBLE
    }

    private final Type resultType;

    @Nullable private final Schema schemaAfterEvolve;
    @Nullable private final List<SchemaChangeEvent> schemaChanges;
    @Nullable private final String incompatibleReason;

    public SchemaEvolveResult(
            Type resultType,
            @Nullable Schema schemaAfterEvolve,
            @Nullable List<SchemaChangeEvent> schemaChanges,
            @Nullable String incompatibleReason) {
        this.resultType = resultType;
        this.schemaAfterEvolve = schemaAfterEvolve;
        this.schemaChanges = schemaChanges;
        this.incompatibleReason = incompatibleReason;
    }

    public static SchemaEvolveResult compatibleAsIs() {
        return new SchemaEvolveResult(Type.COMPATIBLE_AS_IS, null, null, null);
    }

    public static SchemaEvolveResult compatibleAfterEvolution(
            Schema schemaAfterEvolve, List<SchemaChangeEvent> schemaChanges) {
        return new SchemaEvolveResult(
                Type.COMPATIBLE_AFTER_EVOLUTION, schemaAfterEvolve, schemaChanges, null);
    }

    public static SchemaEvolveResult incompatible(String reason) {
        return new SchemaEvolveResult(Type.INCOMPATIBLE, null, null, reason);
    }

    public boolean isCompatibleAsIs() {
        return resultType == Type.COMPATIBLE_AS_IS;
    }

    public boolean isCompatibleAfterEvolution() {
        return resultType == Type.COMPATIBLE_AFTER_EVOLUTION;
    }

    public boolean isIncompatible() {
        return resultType == Type.INCOMPATIBLE;
    }

    @Nullable
    public Schema getSchemaAfterEvolve() {
        return schemaAfterEvolve;
    }

    @Nullable
    public List<SchemaChangeEvent> getSchemaChanges() {
        return schemaChanges;
    }

    @Nullable
    public String getIncompatibleReason() {
        return incompatibleReason;
    }
}
