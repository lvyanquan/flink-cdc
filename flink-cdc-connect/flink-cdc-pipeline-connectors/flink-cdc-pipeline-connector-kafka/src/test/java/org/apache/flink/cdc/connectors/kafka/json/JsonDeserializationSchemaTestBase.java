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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/** Deserialize schema test base for the json format. */
public abstract class JsonDeserializationSchemaTestBase {

    /** A collector that stores elements in a list. */
    public static class SimpleCollector implements Collector<Event> {

        private final List<Event> list = new ArrayList<>();

        public List<Event> getList() {
            return list;
        }

        public void clearList() {
            list.clear();
        }

        @Override
        public void collect(Event event) {
            list.add(event);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
