/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.rest;

import org.apache.flink.gateway.JarBuilder;
import org.apache.flink.table.artifacts.ArtifactIdentifier;
import org.apache.flink.table.artifacts.ArtifactKind;
import org.apache.flink.table.artifacts.FileSystemArtifactFinder;
import org.apache.flink.table.artifacts.FileSystemArtifactFinderFactory;
import org.apache.flink.table.gateway.api.config.Engine;
import org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.vvr.command.ExecutionEnvironment;
import org.apache.flink.table.gateway.api.vvr.command.info.SyntaxInfo;
import org.apache.flink.table.gateway.vvr.rest.message.command.cdc.ConvertCxasToCdcYamlResponseBody;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.artifacts.ArtifactOptions.FILE_SYSTEM_ARTIFACT_FINDER_PATH;
import static org.apache.flink.table.artifacts.CommonArtifactOptions.ARTIFACT_FINDER_IDENTIFIER;
import static org.apache.flink.table.artifacts.CommonArtifactOptions.getArtifactFinderOptionKey;
import static org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions.SQL_GATEWAY_ARTIFACT_REUSE_CLASSLOADER;

/**
 * Test basic logic for {@link
 * org.apache.flink.table.gateway.vvr.rest.handler.command.cdc.ConvertCxasToCdcYamlHandler}.
 */
public class ConvertCxasToYamlITCase extends VvrRestAPICaseBase {

    public ConvertCxasToYamlITCase() {
        super();
    }

    private static @TempDir Path workDir;

    private static final String TEST_CTAS_SQL =
            "CREATE TABLE IF NOT EXISTS\n"
                    + "  `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz`\n"
                    + "WITH (\n"
                    + "  'enableTypeNormalization' = 'true'\n"
                    + ")\n"
                    + "AS TABLE\n"
                    + "  `source_catalog_foo`.`source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                    + "/*+ `OPTIONS`(\n"
                    + "  'server-id' = '5611-5620',\n"
                    + "  'jdbc.properties.tinyInt1isBit' = 'false',\n"
                    + "  'jdbc.properties.transformedBitIsBoolean' = 'false'\n"
                    + ") */";

    private static final String TEST_CDAS_SQL =
            "CREATE DATABASE IF NOT EXISTS\n"
                    + "  `sink_catalog_foo`.`sink_database_bar`\n"
                    + "WITH (\n"
                    + "  'extras' = 'something'\n"
                    + ") AS DATABASE\n"
                    + "  `source_catalog_foo`.`source_database_bar`\n"
                    + "INCLUDING TABLE 'invite_register_records'\n"
                    + "/*+ `OPTIONS`(\n"
                    + "  'server-id' = '7777-7777',\n"
                    + "  'server-time-zone' = 'UTC',\n"
                    + "  'debezium.include.schema.changes' = 'false',\n"
                    + "  'debezium.event.deserialization.failure.handling.mode' = 'warn',\n"
                    + "  'debezium.snapshot.mode' = 'schema_only',\n"
                    + "  'scan.startup.mode' = 'timestamp',\n"
                    + "  'scan.startup.timestamp-millis' = '1728435528000'\n"
                    + ") */";

    private static final String NON_CXAS_SQL =
            "CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`;";

    private static final String MALFORMED_SQL = "CREATE ^_^ DATABASE AS TABLE;";

    @BeforeAll
    static void initializeCatalog() throws Exception {
        createCatalog("source_catalog_foo", "source_database_bar");
        createCatalog("sink_catalog_foo", "sink_database_bar");
    }

    private static Map<String, String> getConfig() {
        Path path = Paths.get(workDir.toString(), "test.yaml");
        Map<String, String> config = new HashMap<>(getFileCatalogStoreConfig());
        config.put(ARTIFACT_FINDER_IDENTIFIER.key(), FileSystemArtifactFinderFactory.IDENTIFIER);
        config.put(
                getArtifactFinderOptionKey(
                        FileSystemArtifactFinderFactory.IDENTIFIER,
                        FILE_SYSTEM_ARTIFACT_FINDER_PATH.key()),
                path.toString());
        config.put(SqlGatewayServiceConfigOptions.SQL_GATEWAY_ENGINE.key(), Engine.VVR.name());
        config.put(SQL_GATEWAY_ARTIFACT_REUSE_CLASSLOADER.key(), "false");
        return config;
    }

    @Test
    void testConvertCtasToYaml() throws Exception {
        Assertions.assertThat(sendAnalyzeSyntaxRequest(TEST_CTAS_SQL))
                .map(SyntaxInfo::getFeatures)
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.COLLECTION)
                .containsExactly(SyntaxInfo.Feature.CTAS);

        Assertions.assertThat(sendConvertCxasToCdcYamlRequest(TEST_CTAS_SQL).getYamlContent())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE TABLE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar`.`sink_table_baz` WITH (\n"
                                + "#   'enableTypeNormalization' = 'true'\n"
                                + "# )\n"
                                + "# AS TABLE `source_catalog_foo`.`source_database_bar`.`source_table_baz_([0-9]|[0-9]{2})`\n"
                                + "# /*+ `OPTIONS`('server-id' = '5611-5620', 'jdbc.properties.tinyInt1isBit' = 'false', 'jdbc.properties.transformedBitIsBoolean' = 'false') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  default-database: source_database_bar\n"
                                + "  jdbc.properties.tinyInt1isBit: 'false'\n"
                                + "  jdbc.properties.transformedBitIsBoolean: 'false'\n"
                                + "  path: "
                                + workDir
                                + "/source_catalog_foo\n"
                                + "  server-id: 5611-5620\n"
                                + "  tables: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n"
                                + "  type: mock-test-filesystem\n"
                                + "sink:\n"
                                + "  default-database: sink_database_bar\n"
                                + "  enableTypeNormalization: 'true'\n"
                                + "  path: "
                                + workDir
                                + "/sink_catalog_foo\n"
                                + "  type: mock-test-filesystem\n"
                                + "route:\n"
                                + "- sink-table: sink_database_bar.sink_table_baz\n"
                                + "  source-table: source_database_bar.source_table_baz_([0-9]|[0-9]{2})\n");
    }

    @Test
    void testConvertCdasToYaml() throws Exception {
        Assertions.assertThat(sendAnalyzeSyntaxRequest(TEST_CDAS_SQL))
                .map(SyntaxInfo::getFeatures)
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.COLLECTION)
                .containsExactly(SyntaxInfo.Feature.CDAS);

        Assertions.assertThat(sendConvertCxasToCdcYamlRequest(TEST_CDAS_SQL).getYamlContent())
                .isEqualTo(
                        "# Converted from the following SQL: \n"
                                + "# \n"
                                + "# CREATE DATABASE IF NOT EXISTS `sink_catalog_foo`.`sink_database_bar` WITH (\n"
                                + "#   'extras' = 'something'\n"
                                + "# ) AS DATABASE `source_catalog_foo`.`source_database_bar`\n"
                                + "# INCLUDING TABLE 'invite_register_records'\n"
                                + "# /*+ `OPTIONS`('server-id' = '7777-7777', 'server-time-zone' = 'UTC', 'debezium.include.schema.changes' = 'false', 'debezium.event.deserialization.failure.handling.mode' = 'warn', 'debezium.snapshot.mode' = 'schema_only', 'scan.startup.mode' = 'timestamp', 'scan.startup.timestamp-millis' = '1728435528000') */\n"
                                + "---\n"
                                + "source:\n"
                                + "  debezium.event.deserialization.failure.handling.mode: warn\n"
                                + "  debezium.include.schema.changes: 'false'\n"
                                + "  debezium.snapshot.mode: schema_only\n"
                                + "  default-database: source_database_bar\n"
                                + "  path: "
                                + workDir
                                + "/source_catalog_foo\n"
                                + "  scan.startup.mode: timestamp\n"
                                + "  scan.startup.timestamp-millis: '1728435528000'\n"
                                + "  server-id: 7777-7777\n"
                                + "  server-time-zone: UTC\n"
                                + "  tables: source_database_bar.invite_register_records\n"
                                + "  type: mock-test-filesystem\n"
                                + "sink:\n"
                                + "  default-database: sink_database_bar\n"
                                + "  extras: something\n"
                                + "  path: "
                                + workDir
                                + "/sink_catalog_foo\n"
                                + "  type: mock-test-filesystem\n"
                                + "route:\n"
                                + "- replace-symbol: <:>\n"
                                + "  sink-table: sink_database_bar.<:>\n"
                                + "  source-table: source_database_bar.\\.*\n");
    }

    private static void createCatalog(String catalogName, String dbName) throws Exception {
        // prepare test filesystem dependency
        Path path = Paths.get(workDir.toString(), "test.yaml");
        FileSystemArtifactFinder artifactFinder = new FileSystemArtifactFinder(path.toString());
        artifactFinder.addArtifacts(
                ArtifactIdentifier.of(ArtifactKind.CATALOG, "mock-test-filesystem"),
                Collections.singletonList(
                        JarBuilder.buildTestFileSystemCatalogJar(workDir).toUri()));

        // create user-defined test-filesystem catalog
        Path catalogPath = Paths.get(workDir.toString(), catalogName);
        Files.createDirectory(catalogPath);
        Path dbPath = catalogPath.resolve(dbName);
        Files.createDirectory(dbPath);

        // create catalog and source table
        SessionHandle handle =
                client.openSession(
                        SessionEnvironment.newBuilder().addSessionConfig(getConfig()).build());
        client.executeStatement(
                        handle,
                        String.format(
                                "CREATE CATALOG %s WITH ("
                                        + "'type' = 'mock-test-filesystem', "
                                        + "'path' = '%s', "
                                        + "'default-database' = '%s')",
                                catalogName, catalogPath, dbName),
                        Collections.emptyMap())
                .get();
        client.closeSession(handle);
    }

    private List<SyntaxInfo> sendAnalyzeSyntaxRequest(String sql) throws Exception {
        return client.analyzeSyntax(
                sql, ExecutionEnvironment.builder().withConfig(getConfig()).build());
    }

    private ConvertCxasToCdcYamlResponseBody sendConvertCxasToCdcYamlRequest(String yamlContent)
            throws Exception {
        return ConvertCxasToCdcYamlResponseBody.from(
                client.convertCxasToCdcYaml(
                                yamlContent,
                                ExecutionEnvironment.builder().withConfig(getConfig()).build())
                        .get());
    }
}
