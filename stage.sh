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
STAGE_MYSQL="mysql"
STAGE_POSTGRES="postgres"
STAGE_ORACLE="oracle"
STAGE_MONGODB="mongodb"
STAGE_SQL_SERVER="sqlserver"
STAGE_TIDB="tidb"
STAGE_OCEANBASE="oceanbase"
STAGE_DB2="db2"
STAGE_VITESS="vitess"
STAGE_PIPELINE_E2E="pipeline_e2e"
STAGE_SOURCE_E2E="source_e2e"

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

MODULES_MYSQL="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc"

MODULES_POSTGRES="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"

MODULES_ORACLE="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc"

MODULES_MONGODB="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mongodb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc"

MODULES_SQLSERVER="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-sqlserver-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc"

MODULES_TIDB="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-tidb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-tidb-cdc"

MODULES_OCEANBASE="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oceanbase-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oceanbase-cdc"

MODULES_DB2="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-db2-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-db2-cdc"

MODULES_VITESS="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-vitess-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-vitess-cdc"

MODULES_PIPELINE_E2E="\
flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests"

MODULES_SOURCE_E2E="\
flink-cdc-e2e-tests/flink-cdc-source-e2e-tests"

function get_compile_modules_for_stage() {
  local stage=$1
  case ${stage} in
      ("core")
       echo "-pl $MODULES_CORE -am"
      ;;
      ("pipeline_connectors")
       echo "-pl $MODULES_PIPELINE_CONNECTORS -am"
      ;;
      ("mysql")
       echo "-pl $MODULES_MYSQL -am"
      ;;
      ("postgres")
       echo "-pl $MODULES_POSTGRES -am"
      ;;
      ("oracle")
       echo "-pl $MODULES_ORACLE -am"
      ;;
      ("mongodb")
       echo "-pl $MODULES_MONGODB -am"
      ;;
      ("sqlserver")
       echo "-pl $MODULES_SQLSERVER -am"
      ;;
      ("tidb")
       echo "-pl $MODULES_TIDB -am"
      ;;
      ("oceanbase")
       echo "-pl $MODULES_OCEANBASE -am"
      ;;
      ("db2")
       echo "-pl $MODULES_DB2 -am"
      ;;
      ("vitess")
       echo "-pl $MODULES_VITESS -am"
      ;;
      ("pipeline_e2e")
        # Compile all
        echo ""
      ;;
      ("source_e2e")
        # Compile all
        echo ""
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
      ("mysql")
       echo "-pl $MODULES_MYSQL"
      ;;
      ("postgres")
       echo "-pl $MODULES_POSTGRES"
      ;;
      ("oracle")
       echo "-pl $MODULES_ORACLE"
      ;;
      ("mongodb")
       echo "-pl $MODULES_MONGODB"
      ;;
      ("sqlserver")
       echo "-pl $MODULES_SQLSERVER"
      ;;
      ("tidb")
       echo "-pl $MODULES_TIDB "
      ;;
      ("oceanbase")
       echo "-pl $MODULES_OCEANBASE"
      ;;
      ("db2")
       echo "-pl $MODULES_DB2"
      ;;
      ("vitess")
       echo "-pl $MODULES_VITESS"
      ;;
      ("pipeline_e2e")
        # Compile all
        echo "-pl $MODULES_PIPELINE_E2E"
      ;;
      ("source_e2e")
        # Compile all
        echo "-pl $MODULES_SOURCE_E2E"
      ;;
    esac
}