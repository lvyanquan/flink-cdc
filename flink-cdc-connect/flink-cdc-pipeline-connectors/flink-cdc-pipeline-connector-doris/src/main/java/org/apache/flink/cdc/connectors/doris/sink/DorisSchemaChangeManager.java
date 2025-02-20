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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.utils.Preconditions;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.doris.flink.catalog.doris.DorisSystem.identifier;

/** An enriched version of Doris' {@link SchemaChangeManager}. */
public class DorisSchemaChangeManager extends SchemaChangeManager {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSchemaChangeManager.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    public DorisSchemaChangeManager(DorisOptions dorisOptions, String charsetEncoding) {
        super(dorisOptions, charsetEncoding);
    }

    public boolean truncateTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String truncateTableDDL =
                "TRUNCATE TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(truncateTableDDL, databaseName);
    }

    public boolean dropTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String dropTableDDL =
                "DROP TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(dropTableDDL, databaseName);
    }

    private JsonNode query(String databaseName, String sql) throws Exception {
        HttpPost httpPost = this.buildHttpPost(sql, databaseName);
        String responseEntity = handleResponse(httpPost);
        return objectMapper.readTree(responseEntity);
    }

    private String handleResponse(HttpUriRequest request) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != 200 || response.getEntity() == null) {
                throw new DorisSchemaChangeException(
                        "Failed to query schemaChange, status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            } else {
                return EntityUtils.toString(response.getEntity());
            }
        } catch (Exception e) {
            LOG.error("Query error.", e);
            throw new DorisSchemaChangeException(
                    "SchemaChange request error with " + e.getMessage());
        }
    }

    public MetadataApplyingState getAlterJobState(String databaseName, String tableName)
            throws Exception {
        String showAlterSql =
                String.format(
                        "SHOW ALTER TABLE COLUMN FROM `%s` WHERE TableName = '%s' ORDER BY JobId DESC LIMIT 1;",
                        databaseName, tableName);

        JsonNode result;

        try {
            result = query(databaseName, showAlterSql);
        } catch (Exception e) {
            throw new SQLException(
                    String.format(
                            "Failed to retrieve alter job state for %s.%s.",
                            databaseName, tableName),
                    e);
        }

        List<String> metaKeys = new ArrayList<>();
        List<String> metaValues = new ArrayList<>();

        result.get("data").get("meta").forEach(node -> metaKeys.add(node.get("name").asText()));

        JsonNode metaValueNode = result.get("data").get("data").get(0);
        if (metaValueNode == null) {
            return MetadataApplyingState.EMPTY;
        }

        metaValueNode.forEach(node -> metaValues.add(node.asText()));

        Preconditions.checkArgument(
                metaKeys.size() == metaValues.size(),
                "Doris meta header (%s) and value array (%s) mismatch.",
                metaKeys,
                metaValues);

        Map<String, String> metaMap = new HashMap<>();
        for (int i = 0; i < metaKeys.size(); i++) {
            metaMap.put(metaKeys.get(i), metaValues.get(i));
        }

        return new MetadataApplyingState(
                metaMap.get("JobId"),
                metaMap.get("TableName"),
                metaMap.get("CreateTime"),
                metaMap.get("FinishTime"),
                metaMap.get("TransactionId"),
                metaMap.get("State"),
                metaMap.get("Msg"));
    }

    /** Status of metadata being applied. */
    public static class MetadataApplyingState {
        String jobId;
        String tableName;
        String createTime;
        String finishTime;
        String transactionId;
        String state;
        String msg;

        @Override
        public String toString() {
            return "AlterJobState{"
                    + "jobId='"
                    + jobId
                    + '\''
                    + ", tableName='"
                    + tableName
                    + '\''
                    + ", createTime='"
                    + createTime
                    + '\''
                    + ", finishTime='"
                    + finishTime
                    + '\''
                    + ", transactionId='"
                    + transactionId
                    + '\''
                    + ", state='"
                    + state
                    + '\''
                    + ", msg='"
                    + msg
                    + '\''
                    + '}';
        }

        public static final MetadataApplyingState EMPTY =
                new MetadataApplyingState(null, null, null, null, null, "EMPTY", null);

        public MetadataApplyingState(
                String jobId,
                String tableName,
                String createTime,
                String finishTime,
                String transactionId,
                String state,
                String msg) {
            this.jobId = jobId;
            this.tableName = tableName;
            this.createTime = createTime;
            this.finishTime = finishTime;
            this.transactionId = transactionId;
            this.state = state;
            this.msg = msg;
        }
    }
}
