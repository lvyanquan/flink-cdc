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

osName=""
vvrVersion=""
scalaVersion=""
connectorHome=""
if [ $# -eq 3 ]; then
  vvrVersion=$1
  scalaVersion=$2
  connectorHome=$3
  osName=$(getOsName)
else
  osName=$1
  vvrVersion=$2
  scalaVersion=$3
  connectorHome=$4
fi
echo "$osName, $vvrVersion, $scalaVersion, $connectorHome"

package() {
  osName=$1
  scalaVersion=$2

  buildCmd="mvn clean install -U -Dmaven.compile.fork=true -DskipTests --batch-mode --errors --show-version -Dcheckstyle.skip -Dgpg.skip -Dmaven.javadoc.skip -Drat.ignoreErrors -Dscala-${scalaVersion} -DskipTests -Prelease -Dververica.password=\"wRa9MVxNfg3@dNbXgpfE8_D?\" -Dververica.salt=\"alibaba-ververica\""

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

buildTar() {
  cp -r tar-building-target/flink-cdc ./
  cp flink-cdc-pipeline-connector-meta.yaml flink-cdc/
  tar zcvf flink-cdc-pipeline-connectors.tar.gz flink-cdc
}

# Copy VVR connectors metadata & artifacts in place
copyVvrConnectors() {
  connectorHome=$1

  cp $connectorHome/image-building-target/flink/flink-connector-meta.yaml tar-building-target/flink/flink-connector-meta.yaml
  cp $connectorHome/image-building-target/flink/flink-connector-metrics.yaml tar-building-target/flink/flink-connector-metrics.yaml

  cp -r $connectorHome/image-building-target/flink/opt/catalogs tar-building-target/flink/opt/catalogs
  cp -r $connectorHome/image-building-target/flink/opt/connectors tar-building-target/flink/opt/connectors
  cp -r $connectorHome/image-building-target/flink/opt/formats tar-building-target/flink/opt/formats

  echo "tar-building-target contents: "
  ls -lR tar-building-target
}

main() {
  osName=$1
  versionName=$2
  connectorHome=$3
  echo "--main:$osName,$versionName,$connectorHome"

  package "$osName" "$scalaVersion"

  buildTar

  copyVvrConnectors $connectorHome
}

main $osName $vvrVersion $connectorHome
