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

package org.apache.flink.cdc.services.conversion.migrator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.services.conversion.migrator.MigrationRule.asIs;
import static org.apache.flink.cdc.services.conversion.migrator.MigrationRule.drop;
import static org.apache.flink.cdc.services.conversion.migrator.MigrationRule.rename;

/** Defines every connector we supports converting for now. */
public enum MigratableConnector {

    // Mock connector is for testing purposes only. None of these config keys except "path" makes
    // any sense.
    MOCK("mock-test-filesystem", true, true, Collections.singletonList(asIs((key) -> true))),

    // MySQL CDC source connector.
    MYSQL(
            "mysql",
            true,
            false,
            Arrays.asList(
                    MigrationRule.asIs(
                            "hostname",
                            "port",
                            "username",
                            "password",
                            "server-id",
                            "server-time-zone",
                            "scan.incremental.snapshot.chunk.size",
                            "scan.snapshot.fetch.size",
                            "scan.startup.mode",
                            "scan.startup.specific-offset.file",
                            "scan.startup.specific-offset.pos",
                            "scan.startup.specific-offset.gtid-set",
                            "scan.startup.timestamp-millis",
                            "scan.incremental.close-idle-reader.enabled",
                            "connect.timeout",
                            "connect.max-retries",
                            "connection.pool.size",
                            "heartbeat.interval",
                            "scan.incremental.snapshot.enabled",
                            "scan.incremental.snapshot.chunk.key-column",
                            "rds.region-id",
                            "rds.access-key-id",
                            "rds.access-key-secret",
                            "rds.db-instance-id",
                            "rds.main-db-id",
                            "rds.download.timeout",
                            "rds.endpoint",
                            "scan.only.deserialize.captured.tables.changelog.enabled",
                            "scan.parallel-deserialize-changelog.enabled"),
                    asIs((key) -> key.startsWith("jdbc.properties.")),
                    asIs((key) -> key.startsWith("debezium.")),
                    drop(
                            "database-name",
                            "table-name") // Needless to specify TableIds specifically in CXAS mode
                    )),

    // Kafka source connector with schema inferencing.
    KAFKA(
            "kafka",
            true,
            false,
            Arrays.asList(
                    MigrationRule.asIs(
                            "properties.bootstrap.servers",
                            "value.format",
                            "topic",
                            "topic-pattern",
                            "properties.group.id",
                            "scan.startup.mode",
                            "scan.startup.specific-offsets",
                            "scan.startup.timestamp-millis",
                            "scan.topic-partition-discovery.interval",
                            "scan.check.duplicated.group.id"),
                    asIs((key) -> key.startsWith("properties.")),
                    rename("format", "value.format"),
                    drop(
                            "format",
                            "key.format",
                            "key.fields",
                            "key.fields-prefix",
                            "value.fields-include",
                            "scan.header-filter"))),

    // Upsert-Kafka sink connector.
    UPSERT_KAFKA(
            "upsert-kafka",
            false,
            true,
            Arrays.asList(
                    MigrationRule.asIs("properties.bootstrap.servers", "topic"),
                    asIs((key) -> key.startsWith("properties.")),
                    drop(
                            "key.format",
                            "key.fields-prefix",
                            "value.format",
                            "value.fields-include",
                            "sink.parallelism",
                            "sink.buffer-flush.max-rows",
                            "sink.buffer-flush.interval"))),

    // StarRocks sink connector.
    STARROCKS(
            "starrocks",
            false,
            true,
            Arrays.asList(
                    MigrationRule.asIs(
                            "jdbc-url",
                            "username",
                            "password",
                            "load-url",
                            "sink.semantic",
                            "sink.buffer-flush.max-bytes",
                            "sink.buffer-flush.max-rows",
                            "sink.buffer-flush.interval-ms",
                            "sink.max-retries",
                            "sink.connect.timeout-ms"),
                    asIs((key) -> key.startsWith("sink.properties")),
                    rename(
                            (key) -> key.startsWith("starrocks.create.table.properties."),
                            (key) ->
                                    key.replace(
                                            "starrocks.create.table.properties.",
                                            "table.create.properties.")),
                    drop("database-name", "table-name"))),

    // Hologres sink connector.
    HOLOGRES(
            "hologres",
            false,
            true,
            Arrays.asList(
                    MigrationRule.asIs(
                            "username",
                            "password",
                            "endpoint",
                            "connection.ssl.mode",
                            "jdbcretrycount",
                            "jdbcretrysleepinitms",
                            "jdbcretrysleepstepms",
                            "jdbcconnectionmaxidlems",
                            "jdbcmetacachettl",
                            "jdbcmetaautorefreshfactor",
                            "mutatetype",
                            "createparttable",
                            "connectionsize",
                            "jdbcwritebatchsize",
                            "jdbcwritebatchbytesize",
                            "jdbcwriteflushinterval",
                            "ignorenullwhenupdate",
                            "connectionpoolname",
                            "remove-u0000-in-text.enabled",
                            "deduplication.enabled",
                            "jdbcenabledefaultfornotnullcolumn"),
                    drop(
                            "tablename",
                            "bulkload",
                            "userpcmode",
                            "partitionrouter",
                            "ignoredelete",
                            "partial-insert.enabled",
                            "type-mapping.timestamp-converting.legacy",
                            "sdkmode"))),

    // Paimon sink connector.
    PAIMON(
            "paimon",
            false,
            true,
            Arrays.asList(
                    // Table Properties
                    rename(
                            Arrays.asList(
                                            "partial-update.ignore-delete",
                                            "auto-create",
                                            "bucket",
                                            "bucket-key",
                                            "full-compaction.delta-commits",
                                            "merge-engine",
                                            "partition.default-name",
                                            "partition.expiration-check-interval",
                                            "partition.expiration-time",
                                            "partition.timestamp-formatter",
                                            "partition.timestamp-pattern",
                                            "snapshot.num-retained.max",
                                            "snapshot.num-retained.min")
                                    ::contains,
                            "table.properties."::concat),

                    // Catalog properties
                    rename(
                            (key) ->
                                    key.equals("metastore")
                                            || key.equals("warehouse")
                                            || key.startsWith("fs.")
                                            || key.startsWith("dlf.")
                                            || key.startsWith("maxcompute."),
                            "catalog.properties."::concat)));

    public final String name;
    public final boolean isSource;
    public final boolean isSink;
    public final List<MigrationRule> rules;

    MigratableConnector(String name, boolean isSource, boolean isSink, List<MigrationRule> rules) {
        this.name = name;
        this.isSource = isSource;
        this.isSink = isSink;
        this.rules = rules;
    }
}
