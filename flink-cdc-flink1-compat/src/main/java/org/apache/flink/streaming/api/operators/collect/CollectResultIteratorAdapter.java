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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.concurrent.CompletableFuture;

/**
 * Compatibility adapter for Flink 1.20. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Internal
public class CollectResultIteratorAdapter<T> extends CollectResultIterator<T> {

    public CollectResultIteratorAdapter(
            AbstractCollectResultBuffer<T> buffer,
            CompletableFuture<OperatorID> operatorIdFuture,
            String accumulatorName,
            int retryMillis) {
        super(buffer, operatorIdFuture, accumulatorName, retryMillis);
    }

    public CollectResultIteratorAdapter(
            AbstractCollectResultBuffer<T> buffer,
            CollectSinkOperator<T> collectSinkOperator,
            String accumulatorName,
            int retryMillis) {
        super(buffer, collectSinkOperator.getOperatorIdFuture(), accumulatorName, retryMillis);
    }
}
