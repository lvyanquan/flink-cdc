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

import org.apache.flink.table.gateway.api.vvr.command.ExecutionEnvironment;
import org.apache.flink.table.gateway.vvr.rest.message.command.GetAdvicesResponseBody;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Test basic logic for {@link
 * org.apache.flink.table.gateway.vvr.rest.handler.command.cdc.GetCdcAdvicesHandler}.
 */
public class GetCdcAdvicesITCase extends VvrRestAPICaseBase {

    public GetCdcAdvicesITCase() {
        super();
    }

    @Test
    void testGetCdcAdvice() throws Exception {
        String yamlContent =
                "source:\n"
                        + "   type: values\n"
                        + "   name: ValuesSource\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "  name: Values Sink\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  name: Sync Value Database to value\n"
                        + "  parallelism: 1";

        sendGetCdcAdviceRequest(yamlContent, Collections.emptyMap()).toAdviceInfo();
    }

    @Test
    void testGetCdcAdviceErrorSyntax() throws Exception {
        String yamlContent =
                "source:\n"
                        + "   type: values\n"
                        + "   name: ValuesSource\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  name: Sync Value Database to value\n"
                        + "  parallelism: 1";
        try {
            sendGetCdcAdviceRequest(yamlContent, Collections.emptyMap()).toAdviceInfo();
            throw new RuntimeException("If throw this exception, the job is executed success");
        } catch (Exception exception) {
            Assertions.assertThat(exception)
                    .hasMessageContaining("Missing required field \"sink\" in pipeline definition");
        }
    }

    private GetAdvicesResponseBody sendGetCdcAdviceRequest(
            String yamlContent, Map<String, String> config) throws Exception {
        return GetAdvicesResponseBody.from(
                client.getCdcAdvices(
                                yamlContent,
                                ExecutionEnvironment.builder().withConfig(config).build())
                        .get());
    }
}
