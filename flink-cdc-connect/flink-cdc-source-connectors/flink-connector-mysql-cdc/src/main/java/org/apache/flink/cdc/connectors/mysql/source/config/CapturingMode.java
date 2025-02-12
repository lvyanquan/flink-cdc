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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Capturing mode of MySQL CDC source.
 *
 * <p>This enumeration describes whether to run snapshot or binlog phase while starting MySQL CDC
 * source.
 *
 * <p>Save this class for dw.
 */
public enum CapturingMode implements DescribedEnum {
    SNAPSHOT_ONLY(text("Create a snapshot for captured tables then stop the source")),
    BINLOG_ONLY(
            text(
                    "Skip the snapshot phase and capture binlog events from a specific starting offset")),
    HYBRID(text("Create a snapshot then read binlog events associated to captured tables"));

    private final InlineElement description;

    CapturingMode(InlineElement description) {
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
