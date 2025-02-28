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

package org.apache.flink.cdc.services.conversion.migrator;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/** Defines how should we migrate existing CXAS connector options to CDC YAML options. */
public interface MigrationRule extends BiConsumer<Map<String, String>, Map<String, String>> {

    /** Migrates existing keys as-is. */
    static MigrationRule asIs(String... keys) {
        return (existing, reduced) -> {
            for (String key : keys) {
                if (existing.containsKey(key)) {
                    reduced.put(key, existing.get(key));
                }
            }
        };
    }

    static MigrationRule asIs(Predicate<String> keyPredicate) {
        return (existing, reduced) -> {
            existing.forEach(
                    (key, value) -> {
                        if (keyPredicate.test(key)) {
                            reduced.put(key, value);
                        }
                    });
        };
    }

    /** Renames a config entry key name. */
    static MigrationRule rename(String oldKey, String newKey) {
        return (existing, reduced) -> {
            if (existing.containsKey(oldKey)) {
                reduced.put(newKey, existing.get(oldKey));
            }
        };
    }

    /** Renames config entries by key name. */
    static MigrationRule rename(
            Predicate<String> oldKeyPredicate, Function<String, String> newKeyFunction) {
        return (existing, reduced) -> {
            for (Map.Entry<String, String> entry : existing.entrySet()) {
                if (oldKeyPredicate.test(entry.getKey())) {
                    reduced.put(newKeyFunction.apply(entry.getKey()), entry.getValue());
                }
            }
        };
    }

    /** Drops config entries with given key name. */
    static MigrationRule drop(String... keys) {
        return (existing, reduced) -> {
            for (String key : keys) {
                if (existing.containsKey(key)) {
                    reduced.remove(key);
                }
            }
        };
    }
}
