#!/usr/bin/env bash
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

# included in all the css scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*


if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export CSS_CONF_DIR="${CSS_CONF_DIR:-"${CSS_HOME}/conf"}"


# mark default conf here
# export CSS_MASTER_HOST=localhost
export CSS_MASTER_PORT=9099

# export CSS_WORKER_HOST=localhost
export CSS_WORKER_PORT=0

# no need to set this (use random port instead)
export CSS_WORKER_PUSH_PORT=0
export CSS_WORKER_FETCH_PORT=0

export MASTER_JAVA_OPTS="-Xmx1024m"
export WORKER_JAVA_OPTS="-Xmx1024m -XX:MaxDirectMemorySize=4096m"

export CSS_WORKER_INSTANCES=1
