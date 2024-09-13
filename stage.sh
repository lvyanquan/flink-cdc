#!/usr/bin/env bash
#/*
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *      http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

STAGE_COMPILE="compile"
STAGE_CORE="core"
STAGE_MODULES_PIPELINE_CONNECTORS="pipeline"
STAGE_MONGODB="mongodb"
STAGE_MYSQL="mysql"
STAGE_POSTGRES="postgres"
STAGE_SOURCE_E2E="source_e2e"
STAGE_PIPELINE_E2E="pipeline_e2e"

MODULES_CORE="\
flink-cdc-cli,\
flink-cdc-common,\
flink-cdc-composer,\
flink-cdc-runtime"

MODULES_PIPELINE_CONNECTORS="\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-values,\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql,\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris,\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-starrocks,\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-kafka,\
flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon"

MODULES_MONGODB="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mongodb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc"

MODULES_MYSQL="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc"

MODULES_POSTGRES="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"

MODULES_SOURCE_E2E="\
flink-cdc-e2e-tests/flink-cdc-source-e2e-tests"

MODULES_PIPELINE_E2E="\
flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests"

function get_compile_modules_for_stage() {
  local stage=$1
  case ${stage} in
      ("core")
       echo "-pl $MODULES_CORE -am"
      ;;
      ("pipeline_connectors")
       echo "-pl $MODULES_PIPELINE_CONNECTORS -am"
      ;;
      ("mongodb")
       echo "-pl $MODULES_MONGODB -am"
      ;;
      ("mysql")
       echo "-pl $MODULES_MYSQL -am"
      ;;
      ("postgres")
       echo "-pl $MODULES_POSTGRES -am"
      ;;
      ("source_e2e")
       # Compile all
       echo
      ;;
      ("pipeline_e2e")
        # Compile all
        echo
      ;;
    esac
}

function get_test_modules_for_stage() {
  local stage=$1
  case ${stage} in
      ("core")
       echo "-pl $MODULES_CORE"
      ;;
      ("pipeline_connectors")
       echo "-pl $MODULES_PIPELINE_CONNECTORS"
      ;;
      ("mongodb")
       echo "-pl $MODULES_MONGODB"
      ;;
      ("mysql")
       echo "-pl $MODULES_MYSQL"
      ;;
      ("postgres")
       echo "-pl $MODULES_POSTGRES"
      ;;
      ("source_e2e")
        echo "-pl $MODULES_SOURCE_E2E"
      ;;
      ("pipeline_e2e")
        echo "-pl $MODULES_PIPELINE_E2E"
      ;;
    esac
}
