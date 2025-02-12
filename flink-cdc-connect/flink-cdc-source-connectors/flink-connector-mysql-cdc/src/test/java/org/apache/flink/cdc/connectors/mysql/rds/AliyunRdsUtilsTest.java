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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit test for {@link AliyunRdsUtils}. */
public class AliyunRdsUtilsTest {

    @Test
    public void generateNextBinlogFileName() {
        Assertions.assertEquals(
                AliyunRdsUtils.generateNextBinlogFileName("mysql-bin.000001"), "mysql-bin.000002");
        Assertions.assertEquals(
                AliyunRdsUtils.generateNextBinlogFileName("mysql-bin.000129"), "mysql-bin.000130");
        Assertions.assertEquals(
                AliyunRdsUtils.generateNextBinlogFileName("mysql-bin.12345678"),
                "mysql-bin.12345679");
        Assertions.assertEquals(
                AliyunRdsUtils.generateNextBinlogFileName("binlog.000129"), "binlog.000130");
        Assertions.assertThrows(
                IllegalStateException.class, () -> AliyunRdsUtils.generateNextBinlogFileName(null));
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> AliyunRdsUtils.generateNextBinlogFileName("20241107"));
    }
}
