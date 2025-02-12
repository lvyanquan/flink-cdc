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

package org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.fetcher.LocalBinlogFileFetcher;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.core.testutils.CommonTestUtils;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mysql.FileUtils.ADMIN_UID;
import static org.apache.flink.cdc.connectors.mysql.FileUtils.CONTAINER_ROOT;
import static org.apache.flink.cdc.connectors.mysql.FileUtils.ORIGIN_PATH;
import static org.apache.flink.cdc.connectors.mysql.FileUtils.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test of {@link SwitchingBinaryLogClient}. */
class SwitchingBinaryLogClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(SwitchingBinaryLogClientTest.class);
    private static final Path MYSQL_PATH;

    static {
        try {
            MYSQL_PATH =
                    createTempDirectory(
                            Paths.get(new URI("file://" + CONTAINER_ROOT)), "mysql-storage-");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static final MySqlContainer MYSQL =
            ((MySqlContainer)
                    new MySqlContainer(MySqlVersion.V5_7)
                            .withConfigurationOverride("docker/server-gtids/my.cnf")
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withCreateContainerCmdModifier(
                                    (Consumer<CreateContainerCmd>)
                                            createContainerCmd ->
                                                    createContainerCmd.withUser(ADMIN_UID))
                            .withLogConsumer(new Slf4jLogConsumer(LOG))
                            .withFileSystemBind(
                                    ORIGIN_PATH + "/" + MYSQL_PATH.getFileName().toString(),
                                    "/var/lib/mysql"));

    private final UniqueDatabase database =
            new UniqueDatabase(MYSQL, "customer", "mysqluser", "mysqlpw");

    @BeforeAll
    static void setup() {
        LOG.info("Starting MySql containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        LOG.info("Container MySql is started.");
    }

    @AfterAll
    static void tearDown() {
        LOG.info("Stopping MySql containers...");
        MYSQL.stop();
    }

    @BeforeEach
    void resetDatabase() {
        database.createAndInitialize();
    }

    @Test
    void testReadingLocalBinlogFiles() throws Exception {
        try (MySqlConnection connection = DebeziumUtils.createMySqlConnection(getSourceConfig())) {
            Path backupDirectory = backupAndPurgeBinlog(connection);
            BinlogOffset startingOffset = BinlogOffset.ofBinlogFilePosition("", 0);
            LocalBinlogFileFetcher fetcher = new LocalBinlogFileFetcher(backupDirectory);

            SwitchingBinaryLogClient client =
                    new SwitchingBinaryLogClient(
                            MYSQL.getHost(),
                            MYSQL.getDatabasePort(),
                            MYSQL.getUsername(),
                            MYSQL.getPassword(),
                            connection,
                            fetcher,
                            startingOffset);

            client.setLocalOnly();

            // Register error catcher
            AtomicReference<Exception> asyncException = registerErrorCatcher(client);

            // Register listener
            WriteOnlyEventListener listener =
                    new WriteOnlyEventListener(database.getDatabaseName(), "customers");

            client.registerEventListener(listener);

            // Start reading
            client.connect(5000);
            waitForReadingState(client, SwitchingBinaryLogClient.ReadingState.REMOTE_READ);

            assertThat(asyncException)
                    .as("No exception should be thrown inside client")
                    .hasValue(null);
            assertThat(listener.rows)
                    .as("All 21 records in table \"customers\" are expected")
                    .hasSize(21);
        }
    }

    @Test
    void testSeekingToStartingOffset() throws Exception {
        try (MySqlConnection connection = DebeziumUtils.createMySqlConnection(getSourceConfig())) {
            Path backupDirectory = backupAndPurgeBinlog(connection);
            // Randomly pick a starting offset
            int numEventsToSkip = ThreadLocalRandom.current().nextInt(2, 3);
            EventAndOffset startingEventAndOffset =
                    getStartingEventAndOffset(backupDirectory, numEventsToSkip);
            LocalBinlogFileFetcher fetcher = new LocalBinlogFileFetcher(backupDirectory);

            SwitchingBinaryLogClient client =
                    new SwitchingBinaryLogClient(
                            MYSQL.getHost(),
                            MYSQL.getDatabasePort(),
                            MYSQL.getUsername(),
                            MYSQL.getPassword(),
                            connection,
                            fetcher,
                            startingEventAndOffset.offset);
            client.setLocalOnly();

            // Register error catcher
            AtomicReference<Exception> asyncException = registerErrorCatcher(client);

            // Register listener
            FirstEventListener listener = new FirstEventListener();
            client.registerEventListener(listener);

            // Start reading
            client.connect(5000);
            waitForReadingState(client, SwitchingBinaryLogClient.ReadingState.REMOTE_READ);
            assertThat(asyncException)
                    .as("No exception should be thrown inside client")
                    .hasValue(null);

            EventHeaderV4 firstEventHeader = listener.firstEvent.getHeader();
            EventHeaderV4 expectedEventHeader = startingEventAndOffset.event.getHeader();
            assertThat(firstEventHeader.getEventType())
                    .isEqualTo(expectedEventHeader.getEventType());
            assertThat(firstEventHeader.getEventLength())
                    .isEqualTo(expectedEventHeader.getEventLength());
        }
    }

    @Test
    void testSwitchingToRemoteRead() throws Exception {
        try (MySqlConnection connection = DebeziumUtils.createMySqlConnection(getSourceConfig())) {
            Path backupDirectory = backupAndPurgeBinlog(connection);
            LocalBinlogFileFetcher fetcher = new LocalBinlogFileFetcher(backupDirectory);
            SwitchingBinaryLogClient client =
                    new SwitchingBinaryLogClient(
                            MYSQL.getHost(),
                            MYSQL.getDatabasePort(),
                            MYSQL.getUsername(),
                            MYSQL.getPassword(),
                            connection,
                            fetcher,
                            BinlogOffset.ofBinlogFilePosition("", 0));

            // Register error catcher
            AtomicReference<Exception> asyncException = registerErrorCatcher(client);

            // Register listeners
            WriteOnlyEventListener listener =
                    new WriteOnlyEventListener(database.getDatabaseName(), "customers");
            client.registerEventListener(listener);
            String newTable = RandomStringUtils.randomAlphabetic(10);
            WriteOnlyEventListener newTableListener =
                    new WriteOnlyEventListener(database.getDatabaseName(), newTable);
            client.registerEventListener(newTableListener);

            client.connect(1000);
            waitForReadingState(client, SwitchingBinaryLogClient.ReadingState.REMOTE_READ);

            assertThat(asyncException)
                    .as("No exception should be thrown inside client")
                    .hasValue(null);
            assertThat(listener.rows)
                    .as("All 21 records in table \"customers\" are expected")
                    .hasSize(21);

            // Create a new table and make insertions. These changes should be caught in the remote
            // read phase
            connection.execute(
                    String.format(
                            "CREATE TABLE %s (\n"
                                    + "  id INTEGER NOT NULL PRIMARY KEY,\n"
                                    + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                                    + "  address VARCHAR(1024),\n"
                                    + "  phone_number VARCHAR(512)\n"
                                    + ")",
                            database.qualifiedTableName(newTable)));
            connection.execute(
                    String.format(
                            "INSERT INTO %s VALUES (15213, 'user_15213', 'Pittsburgh', '123567891234')",
                            database.qualifiedTableName(newTable)));

            CommonTestUtils.waitUtil(
                    () -> newTableListener.getRows().size() >= 1,
                    Duration.ofSeconds(30),
                    "Timeout waiting for insert event in new table");
        }
    }

    private Path backupAndPurgeBinlog(JdbcConnection connection) throws Exception {
        // Force create a new binlog file
        connection.execute("FLUSH LOGS");
        // Get the latest binlog file name
        BinlogOffset currentOffset = DebeziumUtils.currentBinlogOffset(connection);
        String currentBinlogFilename = currentOffset.getFilename();
        // Copy binlog files before the current one to another place
        Path backupDirectory =
                createTempDirectory(
                        Paths.get(new URI("file://" + CONTAINER_ROOT)), "mysql-binlog-backup-");
        try (Stream<Path> stream = Files.list(MYSQL_PATH)) {
            List<Path> binlogFilesToCopy =
                    stream.filter(file -> file.getFileName().toString().startsWith("mysql-bin.0"))
                            .filter(
                                    path ->
                                            !path.getFileName()
                                                    .toString()
                                                    .equals(currentBinlogFilename))
                            .collect(Collectors.toList());
            for (Path src : binlogFilesToCopy) {
                Path dest =
                        Paths.get(
                                backupDirectory.toAbsolutePath().toString(),
                                src.getFileName().toString());
                Files.copy(src, dest);
            }
        }
        // Purge binlog files to the last one
        connection.execute(String.format("PURGE BINARY LOGS TO '%s'", currentBinlogFilename));
        return backupDirectory;
    }

    @SuppressWarnings("SameParameterValue")
    private static void waitForReadingState(
            SwitchingBinaryLogClient client, SwitchingBinaryLogClient.ReadingState state)
            throws TimeoutException, InterruptedException {
        CommonTestUtils.waitUtil(
                () -> client.getReadingState() == state,
                Duration.ofSeconds(60),
                String.format(
                        "Timeout waiting for the client to be disconnected, state is %s now.",
                        client.getReadingState()));
    }

    private AtomicReference<Exception> registerErrorCatcher(SwitchingBinaryLogClient client) {
        AtomicReference<Exception> asyncException = new AtomicReference<>();
        client.registerLifecycleListener(
                new BinaryLogClient.AbstractLifecycleListener() {
                    @Override
                    public void onEventDeserializationFailure(
                            BinaryLogClient client, Exception ex) {
                        if (asyncException.get() == null) {
                            asyncException.set(ex);
                        } else {
                            asyncException.get().addSuppressed(ex);
                        }
                    }
                });
        return asyncException;
    }

    private EventAndOffset getStartingEventAndOffset(Path binlogPath, int numEventsToSkip)
            throws Exception {
        int numEvents = 0;
        try (LocalBinlogFileFetcher fetcher = new LocalBinlogFileFetcher(binlogPath)) {
            while (fetcher.hasNext()) {
                LocalBinlogFile binlogFile = fetcher.next();
                assertThat(binlogFile).isNotNull();
                BinaryLogFileReader reader =
                        new BinaryLogFileReader(Files.newInputStream(binlogFile.getPath()));

                // Read event in the file
                Event event = reader.readEvent();
                while (event != null) {
                    numEvents++;
                    if (numEvents >= numEventsToSkip) {
                        EventHeaderV4 header = event.getHeader();
                        return new EventAndOffset(
                                event,
                                BinlogOffset.ofBinlogFilePosition(
                                        binlogFile.getFilename(), header.getPosition()));
                    }
                    event = reader.readEvent();
                }
            }
        }
        throw new IllegalStateException(
                String.format(
                        "Number of events %d to skip is greater than total binlog events %d",
                        numEventsToSkip, numEvents));
    }

    private MySqlSourceConfig getSourceConfig() {
        return new MySqlSourceConfigFactory()
                .hostname(MYSQL.getHost())
                .port(MYSQL.getDatabasePort())
                .username("root")
                .password(MYSQL.getPassword())
                .databaseList(database.getDatabaseName())
                .tableList("customers")
                .createConfig(0);
    }

    private static class WriteOnlyEventListener implements BinaryLogClient.EventListener {
        private final List<Object[]> rows = new ArrayList<>();
        private final String capturingDatabaseName;
        private final String capturingTableName;
        private Long capturingTableId;

        public WriteOnlyEventListener(String capturingDatabaseName, String capturingTableName) {
            this.capturingDatabaseName = capturingDatabaseName;
            this.capturingTableName = capturingTableName;
        }

        @Override
        public void onEvent(Event event) {
            if (EventType.TABLE_MAP == event.getHeader().getEventType()) {
                TableMapEventData data = event.getData();
                if (data.getDatabase().equalsIgnoreCase(capturingDatabaseName)
                        && data.getTable().equalsIgnoreCase(capturingTableName)) {
                    capturingTableId = data.getTableId();
                }
            }
            if (EventType.EXT_WRITE_ROWS == event.getHeader().getEventType()) {
                WriteRowsEventData data = event.getData();
                if (capturingTableId != null && data.getTableId() == capturingTableId) {
                    rows.addAll(data.getRows());
                }
            }
        }

        public List<Object[]> getRows() {
            return rows;
        }
    }

    private static class FirstEventListener implements BinaryLogClient.EventListener {
        private Event firstEvent;

        @Override
        public void onEvent(Event event) {
            if (firstEvent == null) {
                firstEvent = event;
            }
        }
    }

    private static class EventAndOffset {
        private final Event event;
        private final BinlogOffset offset;

        public EventAndOffset(Event event, BinlogOffset offset) {
            this.event = event;
            this.offset = offset;
        }
    }
}
