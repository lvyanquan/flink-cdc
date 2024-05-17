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

package org.apache.flink.cdc.connectors.polardbx;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Basic class for testing Database Polardbx which supported the mysql protocol. */
public abstract class PolardbxSourceTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PolardbxSourceTestBase.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    protected static final Integer PORT = 3306;
    protected static final String USER_NAME = System.getenv("POLARDB_USERNAME");
    protected static final String PASSWORD = System.getenv("POLARDB_PASSWORD");
    protected static final String HOSTNAME = System.getenv("POLARDB_HOSTNAME");

    protected static String getHost() {
        return HOSTNAME;
    }

    protected static String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%s", HOSTNAME, PORT);
    }

    protected static Connection getJdbcConnection() throws SQLException {
        String jdbcUrl = getJdbcUrl();
        LOG.info("jdbcUrl is :" + jdbcUrl);
        return DriverManager.getConnection(jdbcUrl, USER_NAME, PASSWORD);
    }

    /** initialize database and tables with ${databaseName}.sql for testing. */
    protected static void initializePolardbxTables(
            String ddlName, String dbName, Function<String, Boolean> filter)
            throws InterruptedException {
        final String ddlFile = String.format("ddl/%s.sql", ddlName);
        final URL ddlTestFile = PolardbxSourceTestBase.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        // need to sleep 1s, make sure the jdbc connection can be created
        Thread.sleep(1000);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database if not exists " + dbName);
            statement.execute("use " + dbName + ";");
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .filter(sql -> filter == null || filter.apply(sql))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void dropDatabase(String dbName) {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    protected String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    protected String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + 4);
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    protected static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    protected static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    protected static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }

    protected static String getRandomSuffix() {
        String base = UUID.randomUUID().toString().replaceAll("-", "");
        if (base.length() > 10) {
            return base.substring(0, 11);
        }
        return base;
    }
}
