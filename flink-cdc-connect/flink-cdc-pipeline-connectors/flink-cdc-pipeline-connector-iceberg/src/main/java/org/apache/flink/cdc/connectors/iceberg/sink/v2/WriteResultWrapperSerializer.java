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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** A {@link SimpleVersionedSerializer} for {@link WriteResultWrapper}. */
public class WriteResultWrapperSerializer implements SimpleVersionedSerializer<WriteResultWrapper> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final WriteResultWrapperSerializer INSTANCE = new WriteResultWrapperSerializer();

    private static final TableIdSerializer tableIdSerializer = new TableIdSerializer();

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(WriteResultWrapper writeResultWrapper) throws IOException {
        return new byte[0];
    }

    @Override
    public WriteResultWrapper deserialize(int i, byte[] bytes) throws IOException {
        return null;
    }
}
