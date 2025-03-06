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

package org.apache.flink.cdc.connectors.mysql.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import org.apache.flink.cdc.connectors.mysql.source.MySqlEvolvingSourceDeserializeSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlMergedSourceDeserializeSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.assigners.AssignStrategy;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.EvolvingSourceProvider;
import org.apache.flink.table.connector.source.MergedSourceProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReportProcessingBacklog;
import org.apache.flink.table.connector.source.abilities.SupportsSchemaEvolutionReading;
import org.apache.flink.table.connector.source.abilities.SupportsTableSourceMerge;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SchemaSpec;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.debezium.config.CommonConnectorConfig.TOMBSTONES_ON_DELETE;
import static io.debezium.connector.mysql.MySqlConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.engine.DebeziumEngine.OFFSET_FLUSH_INTERVAL_MS_PROP;
import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.mysql.source.utils.SerializerUtils.getMetadataConverters;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSource} that describes how to create a MySQL binlog source from a logical
 * description.
 */
public class MySqlTableSource
        implements ScanTableSource,
                SupportsReadingMetadata,
                SupportsSchemaEvolutionReading,
                SupportsTableSourceMerge,
                SupportsReportProcessingBacklog {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlTableSource.class);
    private final Set<String> exceptDbzProperties =
            Stream.of(
                            SNAPSHOT_MODE.name(),
                            OFFSET_FLUSH_INTERVAL_MS_PROP,
                            TOMBSTONES_ON_DELETE.name())
                    .collect(Collectors.toSet());

    private final ResolvedSchema physicalSchemaWithSystemData;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String serverId;
    private final String tableName;
    private final ZoneId serverTimeZone;
    private final Properties dbzProperties;
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final Duration connectTimeout;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final StartupOptions startupOptions;
    private final ObjectIdentifier sourceTablePath;
    private final boolean isShardingTable;
    private final boolean scanNewlyAddedTableEnabled;
    private final boolean closeIdleReaders;
    private final Properties jdbcProperties;
    private final Duration heartbeatInterval;
    private Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();
    final boolean skipSnapshotBackFill;
    final boolean parseOnlineSchemaChanges;
    private final boolean useLegacyJsonFormat;
    @Nullable private final AliyunRdsConfig rdsConfig;

    private final boolean scanOnlyDeserializeCapturedTablesChangelog;
    private final boolean assignUnboundedChunkFirst;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Physical schema of the captured tables. */
    protected Map<ObjectPath, ResolvedSchema> tablePhysicalSchemas = new HashMap<>();

    /** Data type that describes the final output of the source. */
    protected Map<ObjectPath, DataType> tableProducedDataTypes = new HashMap<>();

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    /** Flag to indicate whether the source is evolving source. */
    private boolean isEvolvingSource;

    /** Spec to describe the captured tables. */
    protected Set<MySqlTableSpec> capturedTables;

    /** Whether to read changelog as appendOnly. */
    protected boolean readChangelogAsAppend;

    protected boolean scanParallelDeserializeChangelog;

    protected int scanParallelDeserializeHandlerSize;

    private AssignStrategy scanChunkAssignStrategy;

    public MySqlTableSource(
            ResolvedSchema physicalSchemaWithSystemData,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            ZoneId serverTimeZone,
            Properties dbzProperties,
            @Nullable String serverId,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            StartupOptions startupOptions,
            boolean scanNewlyAddedTableEnabled,
            boolean closeIdleReaders,
            Properties jdbcProperties,
            Duration heartbeatInterval,
            ObjectIdentifier sourceTablePath,
            boolean isShardingTable,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackFill,
            boolean parseOnlineSchemaChanges,
            boolean useLegacyJsonFormat,
            @Nullable AliyunRdsConfig rdsConfig,
            boolean scanOnlyDeserializeCapturedTablesChangelog,
            boolean readChangelogAsAppend,
            boolean scanParallelDeserializeChangelog,
            AssignStrategy scanChunkAssignStrategy,
            int scanParallelDeserializeHandlerSize,
            boolean assignUnboundedChunkFirst) {
        this.physicalSchemaWithSystemData = physicalSchemaWithSystemData;
        this.port = port;
        this.hostname = checkNotNull(hostname);
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.serverId = serverId;
        this.serverTimeZone = serverTimeZone;
        this.dbzProperties = dbzProperties;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.startupOptions = startupOptions;
        this.scanNewlyAddedTableEnabled = scanNewlyAddedTableEnabled;
        this.closeIdleReaders = closeIdleReaders;
        this.jdbcProperties = jdbcProperties;
        this.parseOnlineSchemaChanges = parseOnlineSchemaChanges;
        this.heartbeatInterval = heartbeatInterval;
        if (chunkKeyColumn != null) {
            this.chunkKeyColumns.put(new ObjectPath(database, tableName), chunkKeyColumn);
        }
        this.isShardingTable = isShardingTable;
        // Mutable attributes
        this.tablePhysicalSchemas.put(sourceTablePath.toObjectPath(), physicalSchemaWithSystemData);
        this.tableProducedDataTypes.put(
                sourceTablePath.toObjectPath(),
                physicalSchemaWithSystemData.toPhysicalRowDataType());
        this.metadataKeys = Collections.emptyList();
        this.skipSnapshotBackFill = skipSnapshotBackFill;
        this.useLegacyJsonFormat = useLegacyJsonFormat;
        this.sourceTablePath = sourceTablePath;
        this.isEvolvingSource = false;
        this.capturedTables = new HashSet<>();
        capturedTables.add(
                new MySqlTableSpec(
                        sourceTablePath.toObjectPath(),
                        new ObjectPath(database, tableName),
                        isShardingTable));
        this.rdsConfig = rdsConfig;
        this.scanOnlyDeserializeCapturedTablesChangelog =
                scanOnlyDeserializeCapturedTablesChangelog;
        this.readChangelogAsAppend = readChangelogAsAppend;
        this.scanParallelDeserializeChangelog = scanParallelDeserializeChangelog;
        this.scanChunkAssignStrategy = scanChunkAssignStrategy;
        this.scanParallelDeserializeHandlerSize = scanParallelDeserializeHandlerSize;
        this.assignUnboundedChunkFirst = assignUnboundedChunkFirst;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (readChangelogAsAppend) {
            return ChangelogMode.insertOnly();
        } else {
            return ChangelogMode.all();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        List<String> databases = new ArrayList<>();
        List<String> tableNames = new ArrayList<>();
        Map<MySqlTableSpec, DebeziumDeserializationSchema<RowData>> deserializers = new HashMap<>();
        Map<MySqlTableSpec, SchemaSpec> schemas = new HashMap<>();
        Map<MySqlTableSpec, ResolvedSchema> physicalSchemas = new HashMap<>();
        for (MySqlTableSpec tableSpec : capturedTables) {
            ObjectPath tablePathInFlink = tableSpec.getTablePathInFlink();
            ObjectPath tablePathInMySql = tableSpec.getTablePathInMySql();

            databases.add(tablePathInMySql.getDatabaseName());
            // MySQL debezium connector will use the regular expressions to match
            // the fully-qualified table identifiers of tables.
            // We need use "\\." insteadof "." .
            tableNames.add(
                    tablePathInMySql.getDatabaseName() + "\\." + tablePathInMySql.getObjectName());

            ResolvedSchema physicalSchemaWithSystemData =
                    tablePhysicalSchemas.get(tablePathInFlink);
            MetadataConverter[] systemColumnConverters;
            ResolvedSchema physicalSchema;
            if (tableSpec.isShardingTable()) {
                validateShardingTable(physicalSchemaWithSystemData);
                systemColumnConverters =
                        Arrays.stream(MySqlReadableSystemColumn.values())
                                .map(MySqlReadableSystemColumn::getConverter)
                                .toArray(MetadataConverter[]::new);
                physicalSchema = extractPhysicalRowType(physicalSchemaWithSystemData);
            } else {
                systemColumnConverters = new MetadataConverter[0];
                physicalSchema = physicalSchemaWithSystemData;
            }
            physicalSchemas.put(tableSpec, physicalSchema);

            RowType physicalDataType =
                    (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
            DataType producedDataType = tableProducedDataTypes.get(tablePathInFlink);
            final TypeInformation<RowData> typeInfo =
                    scanContext.createTypeInformation(producedDataType);

            MetadataConverter[] metadataConverters =
                    getMetadataConverters(tableSpec.getMetadataKeys());
            final DebeziumDeserializationSchema<RowData> deserializer =
                    RowDataDebeziumDeserializeSchema.newBuilder()
                            .setPhysicalRowType(physicalDataType)
                            .setSystemColumnConverters(systemColumnConverters)
                            .setMetadataConverters(metadataConverters)
                            .setResultTypeInfo(typeInfo)
                            .setServerTimeZone(serverTimeZone)
                            .setUserDefinedConverterFactory(
                                    MySqlDeserializationConverterFactory.instance())
                            .setReadChangelogAsAppend(readChangelogAsAppend)
                            .build();
            deserializers.put(tableSpec, deserializer);
            schemas.put(tableSpec, SchemaSpec.fromRowDataType(producedDataType));
        }

        boolean isMergedSource = capturedTables.size() > 1;

        final MySqlSourceBuilder<?> parallelSourceBuilder =
                isEvolvingSource || isMergedSource
                        ? MySqlSource.<SourceRecord>builder()
                        : MySqlSource.<RowData>builder();
        parallelSourceBuilder
                .hostname(hostname)
                .port(port)
                .databaseList(databases.toArray(new String[0]))
                .tableList(tableNames.toArray(new String[0]))
                .username(username)
                .password(password)
                .serverTimeZone(serverTimeZone.toString())
                .serverId(serverId)
                .splitSize(splitSize)
                .splitMetaGroupSize(splitMetaGroupSize)
                .distributionFactorUpper(distributionFactorUpper)
                .distributionFactorLower(distributionFactorLower)
                .fetchSize(fetchSize)
                .connectTimeout(connectTimeout)
                .connectMaxRetries(connectMaxRetries)
                .connectionPoolSize(connectionPoolSize)
                .debeziumProperties(getParallelDbzProperties(dbzProperties))
                .startupOptions(startupOptions)
                .closeIdleReaders(closeIdleReaders)
                .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                .jdbcProperties(jdbcProperties)
                .heartbeatInterval(heartbeatInterval)
                .skipSnapshotBackfill(skipSnapshotBackFill)
                .parseOnLineSchemaChanges(parseOnlineSchemaChanges)
                .useLegacyJsonFormat(useLegacyJsonFormat)
                .assignUnboundedChunkFirst(assignUnboundedChunkFirst)
                .chunkKeyColumns(chunkKeyColumns)
                .scanOnlyDeserializeCapturedTablesChangelog(
                        scanOnlyDeserializeCapturedTablesChangelog)
                .scanParallelDeserializeChangelog(scanParallelDeserializeChangelog)
                .scanChunkAssignStrategy(scanChunkAssignStrategy)
                .scanParallelDeserializeHandlerSize(scanParallelDeserializeHandlerSize);
        if (rdsConfig != null) {
            parallelSourceBuilder.enableReadingRdsArchivedBinlog(rdsConfig);
        }

        if (isEvolvingSource) {
            final MySqlEvolvingSourceDeserializeSchema evolvingSourceDeserializeSchema =
                    new MySqlEvolvingSourceDeserializeSchema(
                            capturedTables,
                            serverTimeZone,
                            scanContext.getEvolvingSourceTypeInfo(),
                            Boolean.parseBoolean(
                                    jdbcProperties.getProperty("tinyInt1isBit", "true")),
                            readChangelogAsAppend);
            return EvolvingSourceProvider.of(
                    ((MySqlSourceBuilder<SourceRecord>) parallelSourceBuilder)
                            .deserializer(evolvingSourceDeserializeSchema)
                            .build());
        }

        if (isMergedSource) {
            final MySqlMergedSourceDeserializeSchema mergedSourceDeserializeSchema =
                    new MySqlMergedSourceDeserializeSchema(
                            capturedTables,
                            deserializers,
                            schemas,
                            scanContext.getMergedSourceTypeInfo());

            return getMergedSourceProviderWithCheckPK(
                    ((MySqlSourceBuilder<SourceRecord>) parallelSourceBuilder)
                            .deserializer(mergedSourceDeserializeSchema)
                            .build(),
                    physicalSchemas,
                    chunkKeyColumns);
        }

        checkArgument(capturedTables.size() == 1, "There should be one table to scan.");
        MySqlTableSpec tableSpec = capturedTables.iterator().next();
        return getParallelSourceProviderWithCheckPK(
                ((MySqlSourceBuilder<RowData>) parallelSourceBuilder)
                        .deserializer(deserializers.get(tableSpec))
                        .build(),
                physicalSchemas.get(tableSpec),
                chunkKeyColumns.get(tableSpec.getTablePathInMySql()));
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        // Return metadata in a fixed order
        return Stream.of(MySqlReadableMetadata.values())
                .collect(
                        Collectors.toMap(
                                MySqlReadableMetadata::getKey,
                                MySqlReadableMetadata::getDataType,
                                (existingValue, newValue) -> newValue,
                                LinkedHashMap::new));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.tableProducedDataTypes.put(sourceTablePath.toObjectPath(), producedDataType);

        // append metadata to the captured tables, this method happens before applyTableSource
        Set<MySqlTableSpec> tablesWithMetadata = new HashSet<>();
        for (MySqlTableSpec tableSpec : capturedTables) {
            // this method will change the hashcode of table specs
            tableSpec.setMetadataKeys(metadataKeys);
            tablesWithMetadata.add(tableSpec);
        }
        this.capturedTables = tablesWithMetadata;
    }

    @Override
    public DynamicTableSource copy() {
        MySqlTableSource copiedSource =
                new MySqlTableSource(
                        physicalSchemaWithSystemData,
                        port,
                        hostname,
                        database,
                        tableName,
                        username,
                        password,
                        serverTimeZone,
                        dbzProperties,
                        serverId,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectTimeout,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        startupOptions,
                        scanNewlyAddedTableEnabled,
                        closeIdleReaders,
                        jdbcProperties,
                        heartbeatInterval,
                        sourceTablePath,
                        isShardingTable,
                        null,
                        skipSnapshotBackFill,
                        parseOnlineSchemaChanges,
                        useLegacyJsonFormat,
                        rdsConfig,
                        scanOnlyDeserializeCapturedTablesChangelog,
                        readChangelogAsAppend,
                        scanParallelDeserializeChangelog,
                        scanChunkAssignStrategy,
                        scanParallelDeserializeHandlerSize,
                    assignUnboundedChunkFirst);
        copiedSource.tablePhysicalSchemas = new HashMap<>(tablePhysicalSchemas);
        copiedSource.metadataKeys = new ArrayList<>(metadataKeys);
        copiedSource.tableProducedDataTypes = new HashMap<>(tableProducedDataTypes);
        copiedSource.isEvolvingSource = isEvolvingSource;
        copiedSource.capturedTables = new HashSet<>(capturedTables);
        copiedSource.chunkKeyColumns = new HashMap<>(chunkKeyColumns);
        return copiedSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlTableSource)) {
            return false;
        }
        MySqlTableSource that = (MySqlTableSource) o;
        return port == that.port
                && splitSize == that.splitSize
                && splitMetaGroupSize == that.splitMetaGroupSize
                && fetchSize == that.fetchSize
                && distributionFactorUpper == that.distributionFactorUpper
                && distributionFactorLower == that.distributionFactorLower
                && scanNewlyAddedTableEnabled == that.scanNewlyAddedTableEnabled
                && closeIdleReaders == that.closeIdleReaders
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(database, that.database)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(dbzProperties, that.dbzProperties)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(connectMaxRetries, that.connectMaxRetries)
                && Objects.equals(connectionPoolSize, that.connectionPoolSize)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(jdbcProperties, that.jdbcProperties)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
                && Objects.equals(skipSnapshotBackFill, that.skipSnapshotBackFill)
                && parseOnlineSchemaChanges == that.parseOnlineSchemaChanges
                && useLegacyJsonFormat == that.useLegacyJsonFormat
                && Objects.equals(tablePhysicalSchemas, that.tablePhysicalSchemas)
                && Objects.equals(tableProducedDataTypes, that.tableProducedDataTypes)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(isEvolvingSource, that.isEvolvingSource)
                && Objects.equals(sourceTablePath, that.sourceTablePath)
                && Objects.equals(isShardingTable, that.isShardingTable)
                && Objects.equals(capturedTables, that.capturedTables)
                && Objects.equals(chunkKeyColumns, that.chunkKeyColumns)
                && Objects.equals(rdsConfig, that.rdsConfig)
                && Objects.equals(
                        scanOnlyDeserializeCapturedTablesChangelog,
                        that.scanOnlyDeserializeCapturedTablesChangelog)
                && Objects.equals(readChangelogAsAppend, that.readChangelogAsAppend)
                && Objects.equals(
                        scanParallelDeserializeChangelog, that.scanParallelDeserializeChangelog)
                && Objects.equals(scanChunkAssignStrategy, that.scanChunkAssignStrategy)
                && Objects.equals(
                        scanParallelDeserializeHandlerSize,
                        that.scanParallelDeserializeHandlerSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                port,
                hostname,
                database,
                username,
                password,
                serverId,
                tableName,
                serverTimeZone,
                dbzProperties,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                startupOptions,
                tablePhysicalSchemas,
                tableProducedDataTypes,
                metadataKeys,
                scanNewlyAddedTableEnabled,
                closeIdleReaders,
                jdbcProperties,
                heartbeatInterval,
                skipSnapshotBackFill,
                parseOnlineSchemaChanges,
                useLegacyJsonFormat,
                isEvolvingSource,
                sourceTablePath,
                isShardingTable,
                capturedTables,
                chunkKeyColumns,
                rdsConfig,
                readChangelogAsAppend,
                scanParallelDeserializeChangelog,
                scanChunkAssignStrategy,
                scanParallelDeserializeHandlerSize);
    }

    @Override
    public String asSummaryString() {
        return "MySQL-CDC";
    }

    @Override
    public void applySchemaEvolution() {
        isEvolvingSource = true;
    }

    @Override
    public boolean applyTableSource(ScanTableSource anotherScanTableSource) {
        if (anotherScanTableSource instanceof MySqlTableSource) {
            MySqlTableSource anotherMySQLTableSource = (MySqlTableSource) anotherScanTableSource;

            if (hostname.equals(anotherMySQLTableSource.hostname)
                    && port == anotherMySQLTableSource.port
                    && Objects.equals(username, anotherMySQLTableSource.username)
                    && dbzProperties.equals(anotherMySQLTableSource.dbzProperties)
                    && startupOptions.equals(anotherMySQLTableSource.startupOptions)) {

                // For sql source merge case (not CTAS/CDAS), the merge condition is stricter.
                if (!isEvolvingSource) {
                    if (!(serverTimeZone.equals(anotherMySQLTableSource.serverTimeZone)
                                    && splitSize == anotherMySQLTableSource.splitSize
                                    && splitMetaGroupSize
                                            == anotherMySQLTableSource.splitMetaGroupSize
                                    && Math.abs(
                                                    distributionFactorUpper
                                                            - anotherMySQLTableSource
                                                                    .distributionFactorUpper)
                                            < 1e-6
                                    && Math.abs(
                                                    distributionFactorLower
                                                            - anotherMySQLTableSource
                                                                    .distributionFactorLower)
                                            < 1e-6
                                    && fetchSize == anotherMySQLTableSource.fetchSize
                                    && closeIdleReaders == anotherMySQLTableSource.closeIdleReaders
                                    && jdbcProperties.equals(anotherMySQLTableSource.jdbcProperties)
                                    && scanOnlyDeserializeCapturedTablesChangelog
                                            == anotherMySQLTableSource
                                                    .scanOnlyDeserializeCapturedTablesChangelog
                                    && readChangelogAsAppend
                                            == anotherMySQLTableSource.readChangelogAsAppend
                                    && scanParallelDeserializeChangelog
                                            == anotherMySQLTableSource
                                                    .scanParallelDeserializeChangelog)
                            && Objects.equals(
                                    scanChunkAssignStrategy,
                                    anotherMySQLTableSource.scanChunkAssignStrategy)) {
                        return false;
                    }
                }
                tablePhysicalSchemas.putAll(anotherMySQLTableSource.tablePhysicalSchemas);
                tableProducedDataTypes.putAll(anotherMySQLTableSource.tableProducedDataTypes);
                capturedTables.addAll(anotherMySQLTableSource.capturedTables);
                chunkKeyColumns.putAll(anotherMySQLTableSource.chunkKeyColumns);
                return true;
            }
        }
        return false;
    }

    private SourceProvider getParallelSourceProviderWithCheckPK(
            final Source<RowData, ?, ?> source,
            final ResolvedSchema physicalSchema,
            @Nullable final String chunkKeyColumn) {
        return new SourceProvider() {
            public Source<RowData, ?, ?> createSource() {
                validatePrimaryKeyIfEnableParallel(physicalSchema, chunkKeyColumn);
                return source;
            }

            public boolean isBounded() {
                return Boundedness.BOUNDED.equals(source.getBoundedness());
            }
        };
    }

    private MergedSourceProvider getMergedSourceProviderWithCheckPK(
            final Source<SourceRecord, ?, ?> source,
            final Map<MySqlTableSpec, ResolvedSchema> physicalSchemas,
            final Map<ObjectPath, String> chunkKeyColumns) {
        return new MergedSourceProvider() {
            public Source<SourceRecord, ?, ?> createSource() {
                physicalSchemas.forEach(
                        (tableSpec, physicalSchema) ->
                                validatePrimaryKeyIfEnableParallel(
                                        physicalSchema,
                                        chunkKeyColumns.getOrDefault(
                                                tableSpec.getTablePathInMySql(), null)));
                return source;
            }

            public boolean isBounded() {
                return Boundedness.BOUNDED.equals(source.getBoundedness());
            }
        };
    }

    private void validatePrimaryKeyIfEnableParallel(
            ResolvedSchema physicalSchema, @Nullable String chunkKeyColumn) {
        if (chunkKeyColumn == null && !physicalSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    String.format(
                            "'%s' is required for table without primary key.",
                            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key()));
        }
    }

    /**
     * Check the physical schema whether contains all the system columns. Only the sharding table
     * will contains all the system columns.
     */
    private void validateShardingTable(ResolvedSchema physicalSchema) {
        boolean isLegal = false;
        if (physicalSchema.getColumnCount() >= MySqlReadableSystemColumn.values().length) {
            isLegal = true;
            for (MySqlReadableSystemColumn systemData : MySqlReadableSystemColumn.values()) {
                Optional<Column> column = physicalSchema.getColumn(systemData.getIndex());
                if (!column.isPresent()) {
                    isLegal = false;
                    break;
                }
                Column actualColumn = column.get();
                if (!actualColumn.getName().equals(systemData.getKey())
                        || !actualColumn.getDataType().equals(systemData.getDataType())) {
                    isLegal = false;
                    break;
                }
            }
        }
        if (!isLegal) {
            throw new IllegalArgumentException(
                    String.format(
                            "Sharding table should has all the system columns, but get schema: %s.",
                            physicalSchema));
        }
    }

    private ResolvedSchema extractPhysicalRowType(ResolvedSchema inputSchema) {
        return projectSchema(
                inputSchema,
                IntStream.range(
                                MySqlReadableSystemColumn.values().length,
                                inputSchema.getColumnCount())
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new));
    }

    private ResolvedSchema projectSchema(ResolvedSchema tableSchema, int[][] projectedFields) {
        List<Column> columns = new ArrayList<>();
        for (int[] fieldPath : projectedFields) {
            checkArgument(
                    fieldPath.length == 1, "Nested projection push down is not supported yet.");
            Column column = tableSchema.getColumn(fieldPath[0]).get();
            columns.add(column);
        }
        return new ResolvedSchema(
                columns, Collections.emptyList(), tableSchema.getPrimaryKey().orElse(null));
    }

    @VisibleForTesting
    StartupOptions getStartupOptions() {
        return startupOptions;
    }

    @VisibleForTesting
    Properties getDbzProperties() {
        return dbzProperties;
    }

    @VisibleForTesting
    Properties getParallelDbzProperties(Properties dbzProperties) {
        Properties newDbzProperties = new Properties(dbzProperties);
        for (String key : dbzProperties.stringPropertyNames()) {
            if (exceptDbzProperties.contains(key)) {
                LOG.warn("Cannot override debezium option {}.", key);
            } else {
                newDbzProperties.put(key, dbzProperties.get(key));
            }
        }
        return newDbzProperties;
    }
}
