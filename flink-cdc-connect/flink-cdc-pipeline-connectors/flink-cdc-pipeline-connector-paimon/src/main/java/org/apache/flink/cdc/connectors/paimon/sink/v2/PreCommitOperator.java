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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.cdc.connectors.paimon.sink.dlf.DlfCatalogUtil;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** An Operator to add checkpointId to MultiTableCommittable and generate CommittableSummary. */
public class PreCommitOperator
        extends AbstractStreamOperator<CommittableMessage<MultiTableCommittable>>
        implements OneInputStreamOperator<
                CommittableMessage<MultiTableCommittable>,
                CommittableMessage<MultiTableCommittable>> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PreCommitOperator.class);

    private String commitUser;

    private final Options catalogOptions;

    private Catalog catalog;

    private StoreMultiCommitter storeMultiCommitter;

    /** store a list of MultiTableCommittable in one checkpoint. */
    private final List<MultiTableCommittable> multiTableCommittables;

    protected final ReadableConfig flinkConf;

    public PreCommitOperator(Options catalogOptions, String commitUser, ReadableConfig flinkConf) {
        multiTableCommittables = new ArrayList<>();
        this.catalogOptions = catalogOptions;
        this.commitUser = commitUser;
        this.flinkConf = flinkConf;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<MultiTableCommittable>> element) {
        if (catalog == null) {
            DlfCatalogUtil.convertOptionToDlf(catalogOptions, flinkConf);
            this.catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);
            // flinkMetricGroup could be passed after FLIP-371.
            this.storeMultiCommitter =
                    new StoreMultiCommitter(
                            () -> catalog,
                            new org.apache.paimon.flink.sink.Committer.Context() {
                                @Override
                                public String commitUser() {
                                    return commitUser;
                                }

                                @Nullable
                                @Override
                                public OperatorMetricGroup metricGroup() {
                                    return null;
                                }

                                @Override
                                public boolean streamingCheckpointEnabled() {
                                    return true;
                                }

                                @Override
                                public boolean isRestored() {
                                    return false;
                                }

                                @Override
                                public OperatorStateStore stateStore() {
                                    return null;
                                }
                            });
        }
        if (element.getValue() instanceof CommittableWithLineage) {
            multiTableCommittables.add(
                    ((CommittableWithLineage<MultiTableCommittable>) element.getValue())
                            .getCommittable());
        }
    }

    @Override
    public void finish() {
        prepareSnapshotPreBarrier(Long.MAX_VALUE);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        for (int i = 0; i < multiTableCommittables.size(); i++) {
            MultiTableCommittable multiTableCommittable = multiTableCommittables.get(i);
            multiTableCommittables.set(
                    i,
                    new MultiTableCommittable(
                            multiTableCommittable.getDatabase(),
                            multiTableCommittable.getTable(),
                            checkpointId,
                            multiTableCommittable.kind(),
                            multiTableCommittable.wrappedCommittable()));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        multiTableCommittables.forEach(
                (multiTableCommittable) -> {
                    LOGGER.debug(
                            "Try to commit: "
                                    + multiTableCommittable
                                    + " in checkpoint "
                                    + checkpointId);
                });
        WrappedManifestCommittable wrappedManifestCommittable =
                storeMultiCommitter.combine(checkpointId, checkpointId, multiTableCommittables);
        storeMultiCommitter.commit(Arrays.asList(wrappedManifestCommittable));
        LOGGER.debug("Succeeded checkpoint " + checkpointId);
        multiTableCommittables.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (storeMultiCommitter != null) {
            storeMultiCommitter.close();
        }
    }
}
