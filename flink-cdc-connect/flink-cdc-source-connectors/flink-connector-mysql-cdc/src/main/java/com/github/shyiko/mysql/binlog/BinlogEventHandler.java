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

package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.lmax.disruptor.WorkHandler;

/** Convert byte to {@link EventData} parallel. */
public class BinlogEventHandler implements WorkHandler<Event> {

    private EventDeserializer eventDeserializer;

    public BinlogEventHandler(EventDeserializer eventDeserializer) {
        this.eventDeserializer = eventDeserializer;
    }

    /** refer to default case in {@link EventDeserializer#nextEvent(ByteArrayInputStream)}. */
    @Override
    public void onEvent(Event event) throws Exception {
        if (event.getDataBytes() != null) {
            EventDataDeserializer eventDataDeserializer =
                    eventDeserializer.getEventDataDeserializer(event.getHeader().getEventType());
            EventData eventData =
                    eventDeserializer.deserializeEventData(
                            new ByteArrayInputStream(event.getDataBytes()),
                            event.getHeader(),
                            eventDataDeserializer);
            event.setData(eventData);
        }
    }
}
