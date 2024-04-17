/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
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
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSchemaEvolutionReading;
import org.apache.flink.table.connector.source.abilities.SupportsTableSourceMerge;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.SourceRecord;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import com.ververica.cdc.connectors.mysql.source.MySqlEvolvingSourceDeserializeSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlMergedSourceDeserializeSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

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

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.mysql.source.utils.SerializerUtils.getMetadataConverters;
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
                SupportsTableSourceMerge {

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
    private final boolean enableParallelRead;
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
    @Nullable private final AliyunRdsConfig rdsConfig;

    private final boolean scanOnlyDeserializeCapturedTablesChangelog;

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
            boolean enableParallelRead,
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
            @Nullable AliyunRdsConfig rdsConfig,
            boolean scanOnlyDeserializeCapturedTablesChangelog,
            boolean readChangelogAsAppend,
            boolean scanParallelDeserializeChangelog) {
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
        this.enableParallelRead = enableParallelRead;
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
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (!enableParallelRead && isEvolvingSource) {
            throw new IllegalArgumentException(
                    "MySql CDC as evolving source only works for the parallel source.");
        }

        List<String> databases = new ArrayList<>();
        List<String> tableNames = new ArrayList<>();
        Map<MySqlTableSpec, DebeziumDeserializationSchema<RowData>> deserializers = new HashMap<>();
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
            final TypeInformation<RowData> typeInfo =
                    scanContext.createTypeInformation(tableProducedDataTypes.get(tablePathInFlink));

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
        }

        boolean isMergedSource = capturedTables.size() > 1;

        if (enableParallelRead) {
            final MySqlSourceBuilder<?> parallelSourceBuilder =
                    isEvolvingSource || isMergedSource
                            ? MySqlSource.<SourceRecord>builder()
                            : MySqlSource.<RowData>builder();
            // set parameters for parallel source
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
                    .debeziumProperties(dbzProperties)
                    .startupOptions(startupOptions)
                    .closeIdleReaders(closeIdleReaders)
                    .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                    .jdbcProperties(jdbcProperties)
                    .heartbeatInterval(heartbeatInterval)
                    .chunkKeyColumns(chunkKeyColumns)
                    .scanOnlyDeserializeCapturedTablesChangelog(
                            scanOnlyDeserializeCapturedTablesChangelog)
                    .scanParallelDeserializeChangelog(scanParallelDeserializeChangelog);

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
        } else {
            com.ververica.cdc.connectors.mysql.MySqlSource.Builder<RowData> builder =
                    com.ververica.cdc.connectors.mysql.MySqlSource.<RowData>builder()
                            .hostname(hostname)
                            .port(port)
                            .databaseList(database)
                            // MySQL debezium connector will use the regular expressions to match
                            // the fully-qualified table identifiers of tables.
                            // We need use "\\." insteadof "." .
                            .tableList(database + "\\." + tableName)
                            .username(username)
                            .password(password)
                            .serverTimeZone(serverTimeZone.toString())
                            .debeziumProperties(dbzProperties)
                            .startupOptions(startupOptions)
                            .deserializer(deserializers.values().iterator().next());
            Optional.ofNullable(serverId)
                    .ifPresent(serverId -> builder.serverId(Integer.parseInt(serverId)));
            DebeziumSourceFunction<RowData> sourceFunction = builder.build();
            return SourceFunctionProvider.of(sourceFunction, false);
        }
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
                        enableParallelRead,
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
                        rdsConfig,
                        scanOnlyDeserializeCapturedTablesChangelog,
                        readChangelogAsAppend,
                        scanParallelDeserializeChangelog);
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
                && enableParallelRead == that.enableParallelRead
                && splitSize == that.splitSize
                && splitMetaGroupSize == that.splitMetaGroupSize
                && fetchSize == that.fetchSize
                && distributionFactorUpper == that.distributionFactorUpper
                && distributionFactorLower == that.distributionFactorLower
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
                && Objects.equals(scanNewlyAddedTableEnabled, that.scanNewlyAddedTableEnabled)
                && Objects.equals(jdbcProperties, that.jdbcProperties)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
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
                        scanParallelDeserializeChangelog, that.scanParallelDeserializeChangelog);
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
                enableParallelRead,
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
                isEvolvingSource,
                sourceTablePath,
                isShardingTable,
                capturedTables,
                chunkKeyColumns,
                rdsConfig,
                readChangelogAsAppend,
                scanParallelDeserializeChangelog);
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
            if (!(enableParallelRead && anotherMySQLTableSource.enableParallelRead)) {
                return false;
            }

            if (hostname.equals(anotherMySQLTableSource.hostname)
                    && port == anotherMySQLTableSource.port
                    && Objects.equals(username, anotherMySQLTableSource.username)
                    && dbzProperties.equals(anotherMySQLTableSource.dbzProperties)
                    && startupOptions.equals(anotherMySQLTableSource.startupOptions)) {

                // For sql source merge case (not CTAS/CDAS), the merge condition is stricter.
                if (!isEvolvingSource) {
                    if (!(serverTimeZone.equals(anotherMySQLTableSource.serverTimeZone)
                            && splitSize == anotherMySQLTableSource.splitSize
                            && splitMetaGroupSize == anotherMySQLTableSource.splitMetaGroupSize
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
                                    == anotherMySQLTableSource.scanParallelDeserializeChangelog)) {
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
                            "'%s' is required for table without primary key when '%s' enabled.",
                            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                            SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key()));
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
}
