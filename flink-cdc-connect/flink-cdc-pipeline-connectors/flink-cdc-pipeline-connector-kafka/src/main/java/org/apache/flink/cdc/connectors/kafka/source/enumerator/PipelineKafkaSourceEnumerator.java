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

package org.apache.flink.cdc.connectors.kafka.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.kafka.source.PipelineKafkaSource;
import org.apache.flink.cdc.connectors.kafka.source.split.PipelineKafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;

import java.util.Properties;

/** The enumerator class for {@link PipelineKafkaSource}. */
public class PipelineKafkaSourceEnumerator extends KafkaSourceEnumerator {

    public PipelineKafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness);
    }

    public PipelineKafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            KafkaSourceEnumState kafkaSourceEnumState) {
        super(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                kafkaSourceEnumState);
    }

    @Override
    protected KafkaPartitionSplit createKafkaPartitionSplit(
            TopicPartition tp, long startingOffset, long stoppingOffset) {
        return new PipelineKafkaPartitionSplit(tp, startingOffset, stoppingOffset);
    }
}
