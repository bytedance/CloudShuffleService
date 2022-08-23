#!/usr/bin/env bash

#
# Copyright 2022 Bytedance Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -x

dst=$PWD/output

rm -rf ${dst}

latest_commit=$(git log -1 --pretty=oneline | cut -d ' ' -f 1)

if [ -z "${MAVEN_HOME}" ]; then
  echo "maven_home not set..."
fi

export MAVEN_OPTS="-Xmx8g -XX:ReservedCodeCacheSize=2g"

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout $@)
echo "CSS version is $VERSION"

mkdir ${dst}
# Add css jars
mkdir ${dst}/lib

# Build and generate output for spark 3.0
# server also use scala version 2.12
mvn clean package -DskipTests $@
cp service/target/css-service_2.12-*-shaded.jar ${dst}/lib

# Generate bin
mkdir ${dst}/sbin
cp -r sbin/* ${dst}/sbin

# Generate conf
mkdir ${dst}/conf
cp -r conf/* ${dst}/conf

# Generate client
mkdir ${dst}/client

# Build spark shuffle client
mkdir ${dst}/client/spark-3
pushd ./css-assembly_3/target/libs
cp -r *.jar ${dst}/client/spark-3
popd

mvn clean package -Pscala-11 -DskipTests $@
mkdir ${dst}/client/spark-2
pushd ./css-assembly_2/target/libs
cp -r *.jar ${dst}/client/spark-2
popd

# cp mr assembly to dst

# build flink shuffle jar

# cp doc to target output dir
cp -r ./docs ${dst}
cp -r ./README.md ${dst}
cp -r ./LICENSE ${dst}

# Copy commons and examples folder to output
rm -f $PWD/css-*.tgz

TARDIR_NAME="css-$VERSION-bin"
rm -rf ./$TARDIR_NAME
cp -r output $TARDIR_NAME
tar czf "$TARDIR_NAME.tgz" $TARDIR_NAME
rm -rf ./$TARDIR_NAME
cd ../
