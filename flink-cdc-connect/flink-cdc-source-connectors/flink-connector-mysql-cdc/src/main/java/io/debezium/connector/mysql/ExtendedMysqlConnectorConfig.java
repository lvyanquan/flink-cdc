/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

/** Advanced config for better performance. */
public class ExtendedMysqlConnectorConfig {

    /**
     * If set to true, we will only deserialize changelog Events of user defined captured tables in
     * {@link EventDataDeserializer} deserialize method.
     *
     * <p>Defaults to false.
     */
    public static final Field SCAN_ONLY_DESERIALIZE_CAPTURED_TABLES_CHANGELOG_ENABLED =
            Field.create("scan.only.deserialize.captured.tables.changelog.enabled")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.MEDIUM)
                    .withDefault(false)
                    .withDescription(
                            "If set to true, we will only deserialize changelog Events of user defined captured tables, thus we can speed up the binlog process procedure.");
}
