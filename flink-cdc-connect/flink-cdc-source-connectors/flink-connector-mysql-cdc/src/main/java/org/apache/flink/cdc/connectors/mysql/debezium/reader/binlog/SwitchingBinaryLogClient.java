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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.fetcher.BinlogFileFetcher;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import io.debezium.connector.mysql.MySqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A binary log client that first read local binlog files then switch to online mode and start
 * reading from the earliest binlog offset on the database.
 *
 * <p>Please note that we have an assumption here that local binlog files are continuous with online
 * binlog files, which implies that the next binlog position of local binlog files is the earliest
 * offset online.
 */
public class SwitchingBinaryLogClient extends BinaryLogClient {
    private static final Logger LOG = LoggerFactory.getLogger(SwitchingBinaryLogClient.class);
    private final MySqlConnection connection;
    private final BinlogFileFetcher binlogFileFetcher;
    private final BinlogOffset startingOffset;
    private final ExecutorService binlogReadingExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setUncaughtExceptionHandler(this::handleAsyncException)
                            .setNameFormat("local-binlog-reader")
                            .build());
    private EventDeserializer eventDeserializer = new EventDeserializer();
    private volatile ReadingState readingState = ReadingState.LOCAL_SEEK;
    private boolean isLocalOnly = false;

    public SwitchingBinaryLogClient(
            String hostname,
            int port,
            String username,
            String password,
            MySqlConnection connection,
            BinlogFileFetcher binlogFileFetcher,
            BinlogOffset startingOffset) {
        super(hostname, port, username, password);
        this.connection = connection;
        this.binlogFileFetcher = binlogFileFetcher;
        this.startingOffset = startingOffset;
    }

    @Override
    public void connect(long timeout) throws IOException, TimeoutException {
        String earliestBinlogFilename = connection.earliestBinlogFilename();
        FutureUtils.runAfterwardsAsync(
                CompletableFuture.runAsync(
                        () -> readLocalBinlogFiles(earliestBinlogFilename), binlogReadingExecutor),
                () -> connectToRemoteServer(timeout),
                binlogReadingExecutor);
    }

    private void readLocalBinlogFiles(String stoppingBinlogFilename) {
        LOG.info("Start reading local binlog files with startingOffset={}", startingOffset);
        try {
            LocalBinlogFile binlogFile;
            while (binlogFileFetcher.hasNext()) {
                binlogFile = binlogFileFetcher.next();
                if (binlogFile.getFilename().compareToIgnoreCase(stoppingBinlogFilename) >= 0) {
                    break;
                }

                LOG.info("Reading binlog file {}", binlogFile);
                readEventsFromBinlogFile(binlogFile);
            }
            LOG.info("Finished reading local binlog files");
        } catch (Exception e) {
            LOG.error("Error on reading events from local binlog files", e);
            for (LifecycleListener lifecycleListener : getLifecycleListeners()) {
                lifecycleListener.onEventDeserializationFailure(this, e);
            }
        } finally {
            closeBinlogFileFetcher();
        }
    }

    private void connectToRemoteServer(long timeout) throws IOException, TimeoutException {
        checkState(
                readingState == ReadingState.LOCAL_READ,
                "Unexpected reading state %s. This might because the local binlog file reader "
                        + "could not seek to the required starting offset %s",
                readingState,
                startingOffset);
        readingState = ReadingState.REMOTE_READ;
        if (isLocalOnly) {
            disconnect();
            return;
        }
        // Start reading remote binlog from the earliest offset
        setBinlogFilename("");
        setBinlogPosition(0);
        super.connect(timeout);
    }

    // -------------------------- Event deserializer ------------------------

    @Override
    public void setEventDeserializer(EventDeserializer eventDeserializer) {
        super.setEventDeserializer(eventDeserializer);
        this.eventDeserializer = eventDeserializer;
    }

    // ---------------------------- Helper functions ---------------------------

    private void handleAsyncException(Thread t, Throwable e) {
        LOG.error("Exception caught in binlog reading executor. ", e);
        if (!(e instanceof Exception)) {
            throw new RuntimeException("Lifecycle listeners can't accept Throwable", e);
        }
        for (LifecycleListener lifecycleListener : getLifecycleListeners()) {
            lifecycleListener.onEventDeserializationFailure(this, ((Exception) e));
        }
    }

    private void readEventsFromBinlogFile(LocalBinlogFile localBinlogFile) throws IOException {
        try (BinaryLogFileReader reader =
                new BinaryLogFileReader(
                        Files.newInputStream(localBinlogFile.getPath()), eventDeserializer)) {
            Event event = reader.readEvent();
            while (event != null) {
                updateBinlogFilenamePosition(event, localBinlogFile.getFilename());
                // Check if the client is still seeking for starting offset
                if (readingState == ReadingState.LOCAL_SEEK) {
                    if (startingOffset.getFilename() != null) {
                        if (getBinlogFilename().compareToIgnoreCase(startingOffset.getFilename())
                                        >= 0
                                && getBinlogPosition() >= startingOffset.getPosition()) {
                            readingState = ReadingState.LOCAL_READ;
                        }
                    } else {
                        if (event.getHeader().getTimestamp() >= startingOffset.getTimestampSec()) {
                            readingState = ReadingState.LOCAL_READ;
                        }
                    }
                }
                // If the client is already in reading mode, notify the event to listeners
                if (readingState == ReadingState.LOCAL_READ) {
                    notifyEventListeners(event);
                }
                // Read next event
                event = reader.readEvent();
            }
        }
    }

    private void updateBinlogFilenamePosition(Event event, String currentBinlogFilename) {
        setBinlogFilename(currentBinlogFilename);
        EventHeader eventHeader = event.getHeader();
        if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            long position = trackableEventHeader.getPosition();
            if (position > 0) {
                setBinlogPosition(position);
            }
        }
    }

    private void notifyEventListeners(Event event) {
        for (EventListener eventListener : getEventListeners()) {
            eventListener.onEvent(event);
        }
    }

    private void closeBinlogFileFetcher() {
        try {
            binlogFileFetcher.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close binlog file fetcher", e);
        }
    }

    // ----------------------------------- Helper classes ----------------------------------
    @VisibleForTesting
    enum ReadingState {
        LOCAL_SEEK,
        LOCAL_READ,
        REMOTE_READ
    }

    // ----------------------------------- For testing purposes-----------------------------
    @VisibleForTesting
    ReadingState getReadingState() {
        return readingState;
    }

    @VisibleForTesting
    void setLocalOnly() {
        isLocalOnly = true;
    }
}
