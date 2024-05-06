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

package org.apache.flink.cdc.connectors.mysql.rds;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.LocalBinlogFile;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.binlog.fetcher.BinlogFileFetcher;
import org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsConfig;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.aliyun.rds20140815.Client;
import com.aliyun.rds20140815.models.DescribeBinlogFilesRequest;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponse;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponseBody;
import com.aliyun.teaopenapi.models.Config;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Binlog file fetcher that download binlog file from <a
 * href="https://cn.aliyun.com/product/rds/mysql?from_alibabacloud=">RDS</a> archive.
 *
 * <p>Please be careful that the implementation is not thread-safe. Please isolate fetchers across
 * subtasks.
 */
@Internal
@NotThreadSafe
public class AliyunRdsBinlogFileFetcher implements BinlogFileFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(AliyunRdsBinlogFileFetcher.class);
    // Constants
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.of("UTC"));
    private static final int STATUS_CODE_SUCCESS = 200;
    // RDS information
    private final AliyunRdsConfig rdsConfig;
    // Timeout and retries
    private final Duration downloadTimeout;
    // Boundary of fetching binlog files
    private final long startingTimestampMs;
    private final long stoppingTimestampMs;
    // RDS client
    private final Client rdsClient;
    // HTTP client
    private final OkHttpClient httpClient;
    // Directory for holding binlog files
    private final Path binlogFileDirectory;
    // Whether to use intranet download link
    private final boolean useIntranetLink;
    // Queue for holding RDS response
    private final PriorityQueue<RdsBinlogFile> rdsBinlogFileQueue = new PriorityQueue<>();
    // Queue for holding downloaded binlog files
    private final BlockingQueue<LocalBinlogFile> localBinlogFileQueue = new ArrayBlockingQueue<>(2);
    // Executor for downloading binlog files
    private final ExecutorService downloadExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("binlog-file-downloader")
                            .setUncaughtExceptionHandler(this::handleDownloadException)
                            .build());
    private LocalBinlogFile currentServingFile;
    private boolean initialized = false;
    private boolean persistDownloadedFiles = false;
    // Uncaught download exception in download executor
    private volatile Throwable uncaughtDownloadException;
    private volatile boolean isDownloadFinished = false;

    public AliyunRdsBinlogFileFetcher(
            AliyunRdsConfig rdsConfig, long startTimestampMs, long stopTimestampMs) {
        this.rdsConfig = rdsConfig;
        this.downloadTimeout = rdsConfig.getDownloadTimeout();
        this.binlogFileDirectory = rdsConfig.getRandomBinlogDirectoryPath();
        checkArgument(
                Files.isDirectory(binlogFileDirectory),
                "Path '%s' is not a directory",
                this.binlogFileDirectory);
        this.startingTimestampMs = startTimestampMs;
        this.stoppingTimestampMs = stopTimestampMs;
        this.useIntranetLink = rdsConfig.isUseIntranetLink();
        this.rdsClient = initializeRDSClient();
        this.httpClient = initializeHTTPClient();
    }

    @Nullable
    public String initialize() {
        initialized = true;
        retryOnException(
                () -> listBinlogFilesIntoQueue(startingTimestampMs, stoppingTimestampMs),
                downloadTimeout,
                Throwable.class,
                "listBinlogFiles");
        if (rdsBinlogFileQueue.isEmpty()) {
            isDownloadFinished = true;
            return null;
        }
        String earliestBinlogFilename = rdsBinlogFileQueue.peek().getFilename();
        downloadExecutor.execute(
                () -> {
                    LOG.info("Starting binlog file downloading executor");
                    try {
                        while (!rdsBinlogFileQueue.isEmpty()) {
                            RdsBinlogFile rdsBinlogFile = rdsBinlogFileQueue.poll();
                            retryOnException(
                                    () -> downloadBinlogFile(rdsBinlogFile),
                                    downloadTimeout,
                                    Throwable.class,
                                    "downloadBinlogFile");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to download binlog file", e);
                    } finally {
                        isDownloadFinished = true;
                    }
                });
        return earliestBinlogFilename;
    }

    @Override
    public boolean hasNext() {
        checkInitialized();
        boolean hasNext = !(isDownloadFinished && localBinlogFileQueue.isEmpty());
        if (!hasNext && currentServingFile != null) {
            maybeDeleteBinlogFile(currentServingFile);
            currentServingFile = null;
        }
        return hasNext;
    }

    @Override
    public LocalBinlogFile next() {
        checkInitialized();
        checkErrors();
        if (isDownloadFinished && localBinlogFileQueue.isEmpty()) {
            return null;
        }
        if (currentServingFile != null) {
            maybeDeleteBinlogFile(currentServingFile);
        }
        currentServingFile =
                retryOnException(
                        () ->
                                localBinlogFileQueue.poll(
                                        downloadTimeout.toMillis(), TimeUnit.MILLISECONDS),
                        downloadTimeout,
                        InterruptedException.class,
                        "getNextDownloadedFile");
        return currentServingFile;
    }

    @Override
    public void close() throws Exception {
        downloadExecutor.shutdown();
        while (!localBinlogFileQueue.isEmpty()) {
            maybeDeleteBinlogFile(localBinlogFileQueue.poll());
        }
    }

    // -------------------------------------- Helper functions ----------------------------------

    private Client initializeRDSClient() {
        Config config = new Config();
        config.setAccessKeyId(rdsConfig.getAccessKeyId());
        config.setAccessKeySecret(rdsConfig.getAccessKeySecret());
        config.setRegionId(rdsConfig.getRegionId());
        config.setReadTimeout(((int) REQUEST_TIMEOUT.toMillis()));
        config.setConnectTimeout(((int) REQUEST_TIMEOUT.toMillis()));
        try {
            return new Client(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to initialize RDS client", e);
        }
    }

    private OkHttpClient initializeHTTPClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(REQUEST_TIMEOUT)
                .callTimeout(REQUEST_TIMEOUT)
                .readTimeout(REQUEST_TIMEOUT)
                .writeTimeout(REQUEST_TIMEOUT)
                .build();
    }

    private String toDateTime(long epochTimestampMs) {
        Instant instant = Instant.ofEpochMilli(epochTimestampMs);
        return TIMESTAMP_FORMATTER.format(instant);
    }

    private void listBinlogFilesIntoQueue(long startTimestampMs, long stopTimestampMs) {
        LOG.info(
                "Requesting RDS archived binlog files between {} and {}",
                startTimestampMs,
                stopTimestampMs);
        DescribeBinlogFilesRequest request = new DescribeBinlogFilesRequest();
        request.setDBInstanceId(rdsConfig.getDbInstanceId());
        request.setStartTime(toDateTime(startTimestampMs));
        request.setEndTime(toDateTime(stopTimestampMs));
        DescribeBinlogFilesResponse response;
        try {
            response = rdsClient.describeBinlogFiles(request);
            checkState(
                    response.statusCode == STATUS_CODE_SUCCESS,
                    "Unexpected status code %d in the response",
                    response.statusCode);
            response.getBody().getItems().getBinLogFile().stream()
                    .map(payload -> RdsBinlogFile.fromRdsResponsePayload(payload, useIntranetLink))
                    .forEach(rdsBinlogFileQueue::add);
            LOG.info("Received response from RDS: {}", rdsBinlogFileQueue);
            LOG.info(
                    "Approximate binlog size to download: {}",
                    FileUtils.byteCountToDisplaySize(response.getBody().getTotalFileSize()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list binlog files on RDS", e);
        }
    }

    public PriorityQueue<RdsBinlogFile> getRdsBinlogFileQueue() {
        return rdsBinlogFileQueue;
    }

    private LocalBinlogFile downloadBinlogFile(RdsBinlogFile rdsBinlogFile) throws Exception {
        // Create temporary file
        Path localBinlogPath = binlogFileDirectory.resolve(rdsBinlogFile.getFilename());
        LocalBinlogFile localBinlogFile =
                new LocalBinlogFile(rdsBinlogFile.getFilename(), localBinlogPath);
        if (Files.exists(localBinlogPath)) {
            // Already exist binlog file
            LOG.info(
                    "Binlog file {} already exists at {}. Skip fetching",
                    rdsBinlogFile.getFilename(),
                    localBinlogPath);
            return localBinlogFile;
        }
        // Initialize and execute request
        Request request = new Request.Builder().url(rdsBinlogFile.getDownloadLink()).build();
        LOG.info("Downloading binlog file {}", rdsBinlogFile);
        Response response = httpClient.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to download binlog file from %s with status code %d",
                            request.url(), response.code()));
        }
        if (response.body() == null) {
            throw new IllegalStateException(
                    String.format(
                            "Empty response received when downloading binlog file from \"%s\"",
                            request.url()));
        }
        byte[] bytes = response.body().bytes();
        LOG.info(
                "Writing binlog file {} with size {}",
                localBinlogFile,
                FileUtils.byteCountToDisplaySize(bytes.length));
        Files.write(localBinlogFile.getPath(), bytes);
        localBinlogFileQueue.put(localBinlogFile);
        return localBinlogFile;
    }

    private void handleDownloadException(Thread t, Throwable e) {
        if (uncaughtDownloadException == null) {
            uncaughtDownloadException = e;
        } else {
            uncaughtDownloadException.addSuppressed(e);
        }
    }

    private void checkErrors() {
        if (uncaughtDownloadException != null) {
            throw new RuntimeException(
                    "Binlog file downloader encountered exception", uncaughtDownloadException);
        }
    }

    private void maybeDeleteBinlogFile(LocalBinlogFile binlogFile) {
        if (persistDownloadedFiles) {
            return;
        }
        try {
            if (binlogFile != null) {
                Files.deleteIfExists(binlogFile.getPath());
                LOG.info("Binlog file {} is deleted", binlogFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to delete binlog file %s", binlogFile), e);
        }
    }

    private void checkInitialized() {
        checkState(initialized, "RDS fetcher is not initialized");
    }

    private <T, E extends Throwable> T retryOnException(
            SupplierWithException<T, E> supplier,
            Duration timeout,
            Class<E> exceptionClass,
            String actionName) {
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            try {
                return supplier.get();
            } catch (Throwable e) {
                throwUnexpectedException(exceptionClass, actionName, e);
            }
        }
        throw new RuntimeException(
                String.format("Timeout waiting for action %s to complete", actionName));
    }

    @SuppressWarnings("SameParameterValue")
    private <E extends Throwable> void retryOnException(
            ThrowingRunnable<E> runnable,
            Duration timeout,
            Class<E> exceptionClass,
            String actionName) {
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            try {
                runnable.run();
                return;
            } catch (Throwable e) {
                throwUnexpectedException(exceptionClass, actionName, e);
            }
        }
        throw new RuntimeException(
                String.format("Timeout waiting for action %s to complete", actionName));
    }

    private <E extends Throwable> void throwUnexpectedException(
            Class<E> exceptionClass, String actionName, Throwable e) {
        if (!exceptionClass.isInstance(e)) {
            throw new RuntimeException(
                    String.format(
                            "Encountered unexpected exception while doing action %s", actionName));
        } else {
            LOG.warn(
                    String.format(
                            "Retrying %s because of %s", actionName, e.getClass().getSimpleName()),
                    e);
        }
    }

    @VisibleForTesting
    void persistDownloadedFiles() {
        persistDownloadedFiles = true;
    }

    @VisibleForTesting
    Path getBinlogFileDirectory() {
        return binlogFileDirectory;
    }

    // ------------------------------------- Helper classes ----------------------------------

    private static class RdsBinlogFile implements Comparable<RdsBinlogFile> {
        private final String filename;
        private final URL downloadLink;

        public static RdsBinlogFile fromRdsResponsePayload(
                DescribeBinlogFilesResponseBody.DescribeBinlogFilesResponseBodyItemsBinLogFile
                        payload,
                boolean useIntranetDownloadLink) {
            try {
                String downloadLink =
                        useIntranetDownloadLink
                                ? payload.getIntranetDownloadLink()
                                : payload.getDownloadLink();
                return new RdsBinlogFile(payload.getLogFileName(), new URL(downloadLink));
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Malformed binlog file download link: %s",
                                payload.getDownloadLink()),
                        e);
            }
        }

        public RdsBinlogFile(String filename, URL downloadLink) {
            this.filename = filename;
            this.downloadLink = downloadLink;
        }

        public String getFilename() {
            return filename;
        }

        public URL getDownloadLink() {
            return downloadLink;
        }

        @Override
        public String toString() {
            return "RdsBinlogFile{"
                    + "filename='"
                    + filename
                    + '\''
                    + ", downloadLink="
                    + downloadLink
                    + '}';
        }

        @Override
        public int compareTo(@NotNull RdsBinlogFile that) {
            return filename.compareToIgnoreCase(that.filename);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RdsBinlogFile)) {
                return false;
            }
            return filename.equals(((RdsBinlogFile) o).filename);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(filename);
        }
    }
}
