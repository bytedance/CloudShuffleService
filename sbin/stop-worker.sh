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

# A shell script to stop all workers on a single worker
#
# Environment variables
#
#   CSS_WORKER_INSTANCES The number of worker instances that should be
#                          running on this worker machine.  Default is 1.

# Usage: stop-worker.sh
#   Stops all workers on this worker machine

if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

CLASS="com.bytedance.css.service.deploy.worker.Worker"

. "${CSS_HOME}/sbin/css-config.sh"


if [ "$CSS_WORKER_INSTANCES" = "" ]; then
  "${CSS_HOME}/sbin"/css-daemon.sh stop worker 1 $CLASS
else
  for ((i=0; i<$CSS_WORKER_INSTANCES; i++)); do
    "${CSS_HOME}/sbin"/css-daemon.sh stop worker $(( $i + 1 )) $CLASS
  done
fi
