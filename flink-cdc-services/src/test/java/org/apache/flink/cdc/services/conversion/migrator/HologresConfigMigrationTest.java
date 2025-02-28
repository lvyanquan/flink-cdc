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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.services.utils.ServiceTestUtils.buildMap;

/** Test cases for {@link Migrator} with {@code MigratableConnector.HOLOGRES}. */
class HologresConfigMigrationTest {

    @Test
    void testMigratingMinimalConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "hologres")
                                        .put("dbname", "db")
                                        .put("tablename", "tbl")
                                        .put("username", "super_root")
                                        .put("password", "12345678")
                                        .put("endpoint", "001-cn-shenzhen.hologres.aliyuncs.com:80")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "hologres")
                                .put("username", "super_root")
                                .put("password", "12345678")
                                .put("endpoint", "001-cn-shenzhen.hologres.aliyuncs.com:80")
                                .build());
    }

    @Test
    void testMigratingFullConfigurations() {
        Assertions.assertThat(
                        Migrator.migrateSink(
                                buildMap()
                                        .put("type", "hologres")
                                        .put("dbname", "db")
                                        .put("tablename", "tbl")
                                        .put("username", "super_root")
                                        .put("password", "12345678")
                                        .put("endpoint", "001-cn-shenzhen.hologres.aliyuncs.com:80")
                                        .put("connection.ssl.mode", "verify-ca")
                                        .put(
                                                "connection.ssl.root-cert.location",
                                                "/flink/usrlib/certificate.crt")
                                        .put("jdbcretrycount", "11")
                                        .put("jdbcretrysleepinitms", "1001")
                                        .put("jdbcretrysleepstepms", "5001")
                                        .put("jdbcconnectionmaxidlems", "60001")
                                        .put("jdbcmetacachettl", "60001")
                                        .put("jdbcmetaautorefreshfactor", "5")
                                        .put("type-mapping.timestamp-converting.legacy", "false")
                                        .put("property-version", "1")
                                        .put("sdkmode", "rpc")
                                        .put("bulkload", "true")
                                        .put("userpcmode", "true")
                                        .put("mutatetype", "insertorignore")
                                        .put("partitionrouter", "true")
                                        .put("createparttable", "true")
                                        .put("ignoredelete", "false")
                                        .put("sink.delete-strategy", "NON_PK_FIELD_TO_NULL")
                                        .put("connectionsize", "4")
                                        .put("jdbcwritebatchsize", "257")
                                        .put("jdbcwritebatchbytesize", "2097153")
                                        .put("jdbcwriteflushinterval", "10001")
                                        .put("ignorenullwhenupdate", "true")
                                        .put("connectionpoolname", "pool-id")
                                        .put("jdbcenabledefaultfornotnullcolumn", "false")
                                        .put("remove-u0000-in-text.enabled", "true")
                                        .put("partial-insert.enabled", "true")
                                        .put("deduplication.enabled", "false")
                                        .put("check-and-put.column", "id")
                                        .put("check-and-put.operator", "EQUAL")
                                        .put("check-and-put.null-as", "nil")
                                        .put("aggressive.enabled", "true")
                                        .build()))
                .containsExactlyInAnyOrderEntriesOf(
                        buildMap()
                                .put("type", "hologres")
                                .put("username", "super_root")
                                .put("password", "12345678")
                                .put("endpoint", "001-cn-shenzhen.hologres.aliyuncs.com:80")
                                .put("connection.ssl.mode", "verify-ca")
                                .put("jdbcretrycount", "11")
                                .put("jdbcretrysleepinitms", "1001")
                                .put("jdbcretrysleepstepms", "5001")
                                .put("jdbcconnectionmaxidlems", "60001")
                                .put("jdbcmetacachettl", "60001")
                                .put("jdbcmetaautorefreshfactor", "5")
                                .put("mutatetype", "insertorignore")
                                .put("createparttable", "true")
                                .put("connectionsize", "4")
                                .put("jdbcwritebatchsize", "257")
                                .put("jdbcwritebatchbytesize", "2097153")
                                .put("jdbcwriteflushinterval", "10001")
                                .put("ignorenullwhenupdate", "true")
                                .put("connectionpoolname", "pool-id")
                                .put("jdbcenabledefaultfornotnullcolumn", "false")
                                .put("remove-u0000-in-text.enabled", "true")
                                .put("deduplication.enabled", "false")
                                .build());
    }

    @Test
    void testMigratingHologresSource() {
        Assertions.assertThatThrownBy(
                        () ->
                                Assertions.assertThat(
                                        Migrator.migrateSource(
                                                buildMap().put("type", "hologres").build())))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("`hologres` could not be used as a YAML source.");
    }
}
