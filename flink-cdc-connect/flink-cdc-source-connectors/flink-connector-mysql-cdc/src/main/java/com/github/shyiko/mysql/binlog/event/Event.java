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

package com.github.shyiko.mysql.binlog.event;

import java.io.Serializable;

/**
 * Copied from Debezium project(1.9.8.final) to support parallel conversion of byte to {@link
 * Event}.
 *
 * <p>Line 38: add {@link #dataBytes} to deserialize {@link #data} later.
 *
 * <p>Line 38: add {@link #Event()} constructor.
 *
 * <p>Line 85: add {@link #clearAll} to set all member variables to initial status.
 */
public class Event implements Serializable {

    private EventHeader header;
    private EventData data;

    /** stored the original information to deserialize {@link EventData}. */
    private byte[] dataBytes;

    public Event() {}

    public Event(EventHeader header, EventData data) {
        this.header = header;
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    public <T extends EventHeader> T getHeader() {
        return (T) header;
    }

    @SuppressWarnings("unchecked")
    public <T extends EventData> T getData() {
        return (T) data;
    }

    public byte[] getDataBytes() {
        return dataBytes;
    }

    public void setHeader(EventHeader header) {
        this.header = header;
    }

    public void setData(EventData data) {
        this.data = data;
    }

    public void setDataBytes(byte[] dataBytes) {
        this.dataBytes = dataBytes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Event");
        sb.append("{header=").append(header);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }

    // For GC propose.
    public void clearAll() {
        this.header = null;
        this.data = null;
        this.dataBytes = null;
    }
}
