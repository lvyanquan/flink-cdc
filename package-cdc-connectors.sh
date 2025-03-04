#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

function getOsName() {
  name=$(uname -a)
  if [[ "$name" =~ "x86_64" ]]; then
    echo "x86"
  else
    if [[ "$name" =~ "aarch64" ]]; then
      echo "arm"
    fi
  fi
}

cdcConnectors="flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-db2-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-debezium,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mongodb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oceanbase-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-sqlserver-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-test-util,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-tidb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-vitess-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-db2-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oceanbase-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-tidb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-vitess-cdc"

osName=""
vvrVersion=""
scalaVersion=""
if [ $# -eq 2 ]; then
  vvrVersion=$1
  scalaVersion=$2
  osName=$(getOsName)
else
  osName=$1
  vvrVersion=$2
  scalaVersion=$3
fi
echo "$osName, $vvrVersion, $scalaVersion"

package() {
  osName=$1
  scalaVersion=$2

  buildCmd="mvn clean install -U -pl $cdcConnectors -am -Dmaven.compile.fork=true -DskipTests --batch-mode --errors --show-version -Dcheckstyle.skip -Dgpg.skip -Dmaven.javadoc.skip -Drat.ignoreErrors -Dscala-${scalaVersion} -Prelease -Dververica.password=\"wRa9MVxNfg3@dNbXgpfE8_D?\" -Dververica.salt=\"alibaba-ververica\""

  buildArmCmd="$buildCmd -Darch_classifier=aarch_64 "
  java -version
  mvn -version
  if [[ ${osName} == "arm" ]]; then
    eval $buildArmCmd
  else
    eval $buildCmd

  fi
  if [[ $? -ne 0 ]]; then
    echo "package failed!"
    exit 1
  fi
}

main() {
  osName=$1
  versionName=$2
  echo "--main:$osName,$versionName"

  package "$osName" "$scalaVersion"
}

main $osName $vvrVersion
