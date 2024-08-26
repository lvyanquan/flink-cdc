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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.connectors.paimon.sink.dlf.DlfCatalogUtil;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.StoreMultiCommitter;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link Committer} to commit write results for multiple tables. */
public class PaimonCommitter implements Committer<MultiTableCommittable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonCommitter.class);

    private StoreMultiCommitter storeMultiCommitter;

    private String commitUser;

    private final Options catalogOptions;

    private Catalog catalog;

    private ReadableConfig flinkConf;

    public PaimonCommitter(Options catalogOptions, String commitUser, ReadableConfig flinkConf) {
        this.catalogOptions = catalogOptions;
        this.commitUser = commitUser;
        this.flinkConf = flinkConf;
    }

    @Override
    public void commit(Collection<CommitRequest<MultiTableCommittable>> commitRequests) {
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
        if (commitRequests.isEmpty()) {
            return;
        }

        List<MultiTableCommittable> committables =
                commitRequests.stream()
                        .map(CommitRequest::getCommittable)
                        .collect(Collectors.toList());
        // All CommitRequest shared the same checkpointId.
        long checkpointId = committables.get(0).checkpointId();
        int retriedNumber = commitRequests.stream().findFirst().get().getNumberOfRetries();
        WrappedManifestCommittable wrappedManifestCommittable =
                storeMultiCommitter.combine(checkpointId, 1L, committables);
        try {
            if (retriedNumber > 0) {
                storeMultiCommitter.filterAndCommit(
                        Collections.singletonList(wrappedManifestCommittable));
            } else {
                storeMultiCommitter.commit(Collections.singletonList(wrappedManifestCommittable));
            }
            commitRequests.forEach(CommitRequest::signalAlreadyCommitted);
            LOGGER.info(
                    String.format(
                            "Commit succeeded for %s with %s committable",
                            checkpointId, committables.size()));
        } catch (Exception e) {
            commitRequests.forEach(CommitRequest::retryLater);
            LOGGER.warn(
                    String.format(
                            "Commit failed for %s with %s committable",
                            checkpointId, committables.size()));
        }
    }

    @Override
    public void close() throws Exception {
        storeMultiCommitter.close();
    }
}
