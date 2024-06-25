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
jdkVersion=""
vvrVersion=""
scalaVersion=""
if [ $# -eq 3 ]; then
  jdkVersion=$1
  vvrVersion=$2
  scalaVersion=$3
  osName=$(getOsName)
else
  osName=$1
  jdkVersion=$2
  vvrVersion=$3
  scalaVersion=$4
fi
echo "$osName, $jdkVersion, $vvrVersion, $scalaVersion"

package() {
  osName=$1
  scalaVersion=$2
  jdkVersion=$3

  buildCmd="mvn clean -T 4C install -U -Dmaven.compile.fork=true -DskipTests --batch-mode --errors --show-version -Dcheckstyle.skip -Dgpg.skip -Dmaven.javadoc.skip -Drat.ignoreErrors -Dscala-${scalaVersion} -DskipTests -Prelease -Dververica.password=\"wRa9MVxNfg3@dNbXgpfE8_D?\" -Dververica.salt=\"alibaba-ververica\""

  buildArmCmd="$buildCmd -Darch_classifier=aarch_64 "
  if [[ "${jdkVersion}" == "jdk11" ]]; then
    cmd="export JAVA_HOME=${JAVA_HOME_JDK11};export PATH=$JAVA_HOME/bin:$PATH;"
    eval $cmd
  fi
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

main() {
  osName=$1
  jdkVersion=$2
  versionName=$3
  echo "--main:$osName, $jdkVersion,$versionName"

  package "$osName" "$scalaVersion" "$jdkVersion"

  buildTar
}

main $osName $jdkVersion $vvrVersion
