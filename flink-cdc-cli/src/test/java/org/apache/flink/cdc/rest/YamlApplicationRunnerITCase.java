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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.client.deployment.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.api.config.SqlGatewayServiceConfigOptions;
import org.apache.flink.table.gateway.api.vvr.command.DeploymentTarget;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.gateway.vvr.service.application.AESEncryptor;
import org.apache.flink.table.gateway.vvr.service.application.YamlApplicationRunner;
import org.apache.flink.table.gateway.vvr.service.command.DeployDraftCommandBase;
import org.apache.flink.table.runtime.application.cdc.CdcYamlDriver;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_1;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase to verify {@link YamlApplicationRunner}. */
public class YamlApplicationRunnerITCase {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    protected static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(Configuration::new);

    @TempDir static Path resourcePlanDir;
    private static Map<String, String> originalEnv;

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeAll
    static void beforeAll(@TempDir File flinkHome) throws Exception {
        CdcYamlDriver.enableTestMode();

        // prepare yaml
        File confYaml = new File(flinkHome, "flink-conf.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing flink-conf.yaml file.");
        }
        // adjust the test environment for the purposes of this test
        originalEnv = System.getenv();
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, flinkHome.getAbsolutePath());
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(map);
    }

    @AfterAll
    static void cleanup() {
        CdcYamlDriver.disableTestMode();
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(originalEnv);
    }

    @BeforeEach
    void beforeEach() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    void afterEach() {
        System.setOut(standardOut);
    }

    private static Supplier<DispatcherResourceManagerComponentFactory>
            createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                    Configuration configuration, PackagedProgram program) {
        return () -> {
            final ApplicationDispatcherLeaderProcessFactoryFactory
                    applicationDispatcherLeaderProcessFactoryFactory =
                            ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                    new Configuration(configuration),
                                    SessionDispatcherFactory.INSTANCE,
                                    program);
            return new DefaultDispatcherResourceManagerComponentFactory(
                    new DefaultDispatcherRunnerFactory(
                            applicationDispatcherLeaderProcessFactoryFactory),
                    StandaloneResourceManagerFactory.getInstance(),
                    JobRestEndpointFactory.INSTANCE);
        };
    }

    @Test
    void testRunYamlJobInApplicationMode(@TempDir Path temporaryPath) throws Exception {
        String yamlContent =
                "source:\n"
                        + "   type: values\n"
                        + "   name: ValuesSource\n"
                        + "   event-set.id: SINGLE_SPLIT_SINGLE_TABLE\n"
                        + "\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "  name: Values Sink\n"
                        + "  materialized.in.memory: true\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  name: Sync Value Database to value\n"
                        + "  parallelism: 1";
        JobID jobID = JobID.generate();
        String encryptedYamlContent =
                AESEncryptor.generateEncryptorFromJobID(jobID).encrypt(yamlContent);
        assertThat(encryptedYamlContent).isNotEqualTo(yamlContent);
        Path yamlSrciptPath =
                Files.write(
                        temporaryPath.resolve("test_cdc.yaml"), encryptedYamlContent.getBytes());

        runYamlJob(
                yamlSrciptPath.toString(),
                Configuration.fromMap(
                        Collections.singletonMap(
                                SqlGatewayServiceConfigOptions.SQL_GATEWAY_STATEMENT_ENCRYPT_ENABLED
                                        .key(),
                                "true")),
                null,
                false,
                (cluster, actualID) -> {
                    assertThat(actualID).isEqualTo(jobID);
                    awaitJobStatus(cluster, jobID, JobStatus.FINISHED);
                    // Check result in ValuesDatabase
                    List<String> results = ValuesDatabase.getResults(TABLE_1);
                    assertThat(results)
                            .contains(
                                    "default_namespace.default_schema.table1:col1=2;newCol3=x",
                                    "default_namespace.default_schema.table1:col1=3;newCol3=");

                    // Check the order and content of all received events
                    String[] outputEvents = outCaptor.toString().trim().split("\n");
                    assertThat(outputEvents)
                            .containsExactly(
                                    "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                                    "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                                    "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                                    "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}");
                },
                jobID);
    }

    private void runYamlJob(
            String scriptPath,
            Configuration executionConfig,
            @Nullable String resourcePlanPath,
            boolean checkWrittenBack,
            BiConsumerWithException<MiniCluster, JobID, Exception> checker,
            JobID jobId)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        DeployDraftCommandBase.addExecutionConfig(
                DeploymentTarget.APPLICATION_CLUSTER, new JobID(), configuration);
        configuration.addAll(executionConfig);

        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        List<String> arguments = new ArrayList<>();
        arguments.add("--scriptPath");
        arguments.add(scriptPath);

        if (resourcePlanPath != null) {
            arguments.add("--resourcePlan");
            arguments.add(
                    YamlApplicationRunner.class
                            .getClassLoader()
                            .getResource("plan/" + resourcePlanPath)
                            .getPath());

            arguments.add("--appliedResourcePlan");
            arguments.add(resourcePlanDir + "/" + resourcePlanPath);
        }

        PackagedProgram.Builder builder =
                PackagedProgram.newBuilder()
                        .setEntryPointClassName(CdcYamlDriver.class.getName())
                        .setArguments(arguments.toArray(new String[0]));
        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(), builder.build()));

        try (final MiniCluster cluster = clusterBuilder.build()) {

            // start mini cluster and submit the job
            cluster.start();

            // wait until job is running
            checker.accept(cluster, jobId);
        }
        if (checkWrittenBack) {
            File rpFile = Paths.get(resourcePlanDir.toString(), resourcePlanPath).toFile();
            assertThat(rpFile.exists()).isTrue();
        }
    }

    private static void awaitJobStatus(MiniCluster cluster, JobID jobId, JobStatus status)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    try {
                        return cluster.getJobStatus(jobId).get() == status;
                    } catch (ExecutionException e) {
                        if (ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent()) {
                            // job may not be yet submitted
                            return false;
                        }
                        throw e;
                    }
                },
                500,
                60);
    }
}
