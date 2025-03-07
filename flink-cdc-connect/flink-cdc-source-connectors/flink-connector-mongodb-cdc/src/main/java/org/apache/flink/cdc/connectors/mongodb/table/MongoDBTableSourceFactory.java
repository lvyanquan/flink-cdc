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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.OptionSnapshot;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.CannotMigrateException;
import org.apache.flink.table.factories.CannotSnapshotException;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.OptionUpgradableTableFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ASSIGN_ENDING_CHUNK_FIRST;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.COLLECTION;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.CONNECTION_OPTIONS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.HOSTS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.INITIAL_SNAPSHOTTING_QUEUE_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.POLL_MAX_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_FLATTEN_NESTED_COLUMNS_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_NO_CURSOR_TIMEOUT;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCAN_PRIMITIVE_AS_STRING;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.SCHEME;
import static org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for creating configured instance of {@link MongoDBTableSource}. */
public class MongoDBTableSourceFactory
        implements DynamicTableSourceFactory, OptionUpgradableTableFactory {

    private static final String IDENTIFIER = "mongodb-cdc";

    private static final String DOCUMENT_ID_FIELD = "_id";

    private static final int CURRENT_SNAPSHOT_VERSION = 1;

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String scheme = config.get(SCHEME);
        String hosts = config.get(HOSTS);
        String connectionOptions = config.getOptional(CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);

        String database = config.getOptional(DATABASE).orElse(null);
        String collection = config.getOptional(COLLECTION).orElse(null);

        Integer batchSize = config.get(BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis = config.get(HEARTBEAT_INTERVAL_MILLIS);

        StartupOptions startupOptions = getStartupOptions(config);
        Integer initialSnapshottingQueueSize =
                config.getOptional(INITIAL_SNAPSHOTTING_QUEUE_SIZE).orElse(null);

        String zoneId = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        ZoneId localTimeZone =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneId);

        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        boolean enableCloseIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill = config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean assignUnboundedChunkFirst =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_ASSIGN_ENDING_CHUNK_FIRST);

        int splitSizeMB = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int samplesPerChunk = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);

        boolean enableFullDocumentPrePostImage =
                config.getOptional(FULL_DOCUMENT_PRE_POST_IMAGE).orElse(false);

        boolean noCursorTimeout = config.getOptional(SCAN_NO_CURSOR_TIMEOUT).orElse(true);
        boolean flattenNestedColumns =
                config.getOptional(SCAN_FLATTEN_NESTED_COLUMNS_ENABLED).orElse(false);
        boolean primitiveAsString = config.getOptional(SCAN_PRIMITIVE_AS_STRING).orElse(false);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        /*
        checkArgument(physicalSchema.getPrimaryKey().isPresent(), "Primary key must be present");
        checkPrimaryKey(physicalSchema.getPrimaryKey().get(), "Primary key must be _id field");

        Users can create the Mongo cdc source and Mongo lookup source with the same identifier 'mongodb' in flink-connector-mongodb.
        Mongo cdc source request primary key must be '_id', Mongo lookup source has no limit on this.
        But `createDynamicTableSource` will fail for the tables without primary key or primary key is not '_id'.
        We skip the primary key validation for cdc source here.
        The validation is moved to the SourceProvider/SourceFunctionProvider when merging sources.
         */

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return new MongoDBTableSource(
                physicalSchema,
                scheme,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                startupOptions,
                initialSnapshottingQueueSize,
                batchSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone,
                enableParallelRead,
                splitMetaGroupSize,
                splitSizeMB,
                samplesPerChunk,
                enableCloseIdleReaders,
                enableFullDocumentPrePostImage,
                noCursorTimeout,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                flattenNestedColumns,
                primitiveAsString,
                context.getObjectIdentifier(),
                assignUnboundedChunkFirst);
    }

    private void checkPrimaryKey(UniqueConstraint pk, String message) {
        checkArgument(
                pk.getColumns().size() == 1 && pk.getColumns().contains(DOCUMENT_ID_FIELD),
                message);
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        checkNotNull(
                                config.get(SCAN_STARTUP_TIMESTAMP_MILLIS),
                                String.format(
                                        "To use timestamp startup mode, the startup timestamp millis '%s' must be set.",
                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key())));
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECTION_OPTIONS);
        options.add(DATABASE);
        options.add(COLLECTION);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(INITIAL_SNAPSHOTTING_QUEUE_SIZE);
        options.add(BATCH_SIZE);
        options.add(POLL_MAX_BATCH_SIZE);
        options.add(POLL_AWAIT_TIME_MILLIS);
        options.add(HEARTBEAT_INTERVAL_MILLIS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(FULL_DOCUMENT_PRE_POST_IMAGE);
        options.add(SCAN_NO_CURSOR_TIMEOUT);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        // ScanNewlyAddedTable isn't stable for now. We don't expose this option until VVR-62844936
        // got closed.
        // options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SCAN_FLATTEN_NESTED_COLUMNS_ENABLED);
        options.add(SCAN_PRIMITIVE_AS_STRING);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ASSIGN_ENDING_CHUNK_FIRST);
        return options;
    }

    // These options could not be altered during option migrations.
    public Set<String> notAllowedChangedOptions() {
        return Stream.of(
                        DATABASE,
                        COLLECTION,
                        SCAN_STARTUP_MODE,
                        SCAN_STARTUP_TIMESTAMP_MILLIS,
                        SCAN_INCREMENTAL_SNAPSHOT_ENABLED,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES,
                        SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED,
                        SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP,
                        CHUNK_META_GROUP_SIZE,
                        FULL_DOCUMENT_PRE_POST_IMAGE)
                .map(ConfigOption::key)
                .collect(Collectors.toSet());
    }

    @Override
    public OptionSnapshot snapshotOptions(UpgradeContext context) throws CannotSnapshotException {
        Map<String, String> allOptions = new HashMap<>(context.getCatalogTable().getOptions());
        allOptions.put(
                FactoryUtil.PROPERTY_VERSION.key(), String.valueOf(CURRENT_SNAPSHOT_VERSION));
        // Delete password when saving snapshot
        allOptions.remove(PASSWORD.key());
        return new OptionSnapshot(allOptions);
    }

    @Override
    public Map<String, String> migrateOptions(UpgradeContext context, OptionSnapshot snapshot)
            throws CannotMigrateException {
        FactoryUtil.OptionMigrateHelper helper =
                FactoryUtil.createOptionMigrateHelper(this, context);
        Map<String, String> mergedConfig = Configuration.fromMap(snapshot.getOptions()).toMap();
        // check whether the snapshot version is compatible
        if (!String.valueOf(CURRENT_SNAPSHOT_VERSION)
                .equals(mergedConfig.get(FactoryUtil.PROPERTY_VERSION.key()))) {
            throw new CannotMigrateException(
                    String.format(
                            "Can not migrate connector because its snapshot version is %s and current version is %s.",
                            mergedConfig.get(FactoryUtil.PROPERTY_VERSION.key()),
                            CURRENT_SNAPSHOT_VERSION));
        }
        mergedConfig.remove(FactoryUtil.PROPERTY_VERSION.key());
        Map<String, String> enrichmentOptions = helper.getEnrichmentOptions().toMap();
        return checkAndMigrateOptions(notAllowedChangedOptions(), mergedConfig, enrichmentOptions);
    }

    public Map<String, String> checkAndMigrateOptions(
            Set<String> notAllowedChangedOptions,
            Map<String, String> snapshotOptions,
            Map<String, String> newOptions)
            throws CannotMigrateException {
        Set<String> addedOptions = new HashSet<>(newOptions.keySet());
        addedOptions.removeAll(snapshotOptions.keySet());
        Map<String, String> originalOptions = new HashMap<>(snapshotOptions);
        Set<String> removedOptions =
                originalOptions.keySet().stream()
                        .filter(option -> !newOptions.containsKey(option))
                        .collect(Collectors.toSet());
        for (String key : removedOptions) {
            if (notAllowedChangedOptions.contains(key)) {
                throw new CannotMigrateException(
                        String.format(
                                "Can not migrate connector because the option %s is removed.",
                                key));
            }
            // remove the option.
            originalOptions.remove(key);
        }
        for (String key : addedOptions) {
            if (notAllowedChangedOptions.contains(key)) {
                throw new CannotMigrateException(
                        String.format(
                                "Can not migrate connector because the option %s is added.", key));
            }
        }
        for (String key : snapshotOptions.keySet()) {
            if (notAllowedChangedOptions.contains(key)
                    && (!snapshotOptions.get(key).equals(newOptions.get(key)))) {
                throw new CannotMigrateException(
                        String.format(
                                "Can not migrate connector because the option %s is changed from %s to %s.",
                                key, snapshotOptions.get(key), newOptions.get(key)));
            }
        }
        originalOptions.putAll(newOptions);
        return originalOptions;
    }
}
