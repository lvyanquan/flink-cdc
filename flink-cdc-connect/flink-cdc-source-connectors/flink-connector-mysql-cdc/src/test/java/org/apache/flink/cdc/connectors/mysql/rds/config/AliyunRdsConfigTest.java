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

package org.apache.flink.cdc.connectors.mysql.rds.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_ACCESS_KEY_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_ACCESS_KEY_SECRET;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_DB_INSTANCE_ID;
import static org.apache.flink.cdc.connectors.mysql.rds.config.AliyunRdsOptions.RDS_REGION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test of {@link AliyunRdsConfig}. */
class AliyunRdsConfigTest {

    @Test
    void testCreateFromConfig() {
        Configuration configuration = new Configuration();
        // Set required options only
        configuration.set(RDS_REGION_ID, "fake-region");
        configuration.set(RDS_ACCESS_KEY_ID, "fake-key");
        configuration.set(RDS_ACCESS_KEY_SECRET, "fake-secret");
        configuration.set(RDS_DB_INSTANCE_ID, "fake-instance-id");
        AliyunRdsConfig aliyunRdsConfig = AliyunRdsConfig.fromConfig(configuration);
        assertThat(aliyunRdsConfig.getRegionId()).isEqualTo("fake-region");
        assertThat(aliyunRdsConfig.getAccessKeyId()).isEqualTo("fake-key");
        assertThat(aliyunRdsConfig.getAccessKeySecret()).isEqualTo("fake-secret");
        assertThat(aliyunRdsConfig.getDbInstanceId()).isEqualTo("fake-instance-id");
        assertThat(aliyunRdsConfig.getRandomBinlogDirectoryPath()).exists();
        assertThat(aliyunRdsConfig.getDownloadTimeout())
                .isEqualTo(AliyunRdsOptions.RDS_DOWNLOAD_TIMEOUT.defaultValue());
    }
}
