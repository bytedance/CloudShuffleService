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

if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

CLASS="com.bytedance.css.service.deploy.worker.Worker"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-worker.sh <master> <args...>"
  pattern="Usage:"
  pattern+="\|Using CSS's default log4j profile:"
  pattern+="\|Started daemon with process name"
  pattern+="\|Registered signal handler for"

  "${CSS_HOME}"/sbin/css-daemon.sh start worker 1 $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

. "${CSS_HOME}/sbin/css-config.sh"

# First argument should be the master; we need to store it aside because we may
# need to insert arguments between it and the other arguments
MASTER=$1
shift

ORIGINAL_ARGS="$@"

if [ "$WORKER_JAVA_OPTS" != "" ]; then
  JAVA_OPTS=$WORKER_JAVA_OPTS
  export JAVA_OPTS=$JAVA_OPTS
else
  export JAVA_OPTS=""
fi


# Start up the appropriate number of workers on this machine.
# quick local function to start a worker
function start_instance {
  WORKER_NUM=$1
  shift

  if [ "$CSS_WORKER_PORT" = "" ] || [ "$CSS_WORKER_PORT" = "0" ]; then
    PORT_FLAG=
    PORT_NUM=
  else
    PORT_FLAG="--port"
    PORT_NUM=$(( $CSS_WORKER_PORT - $WORKER_NUM + 1 ))
  fi

  if [ "$CSS_WORKER_PUSH_PORT" = "" ] || [ "$CSS_WORKER_PUSH_PORT" = "0" ]; then
    PUSH_PORT_FLAG=
    PUSH_PORT_NUM=
  else
    PUSH_PORT_FLAG="--pushPort"
    PUSH_PORT_NUM=$(( $CSS_WORKER_PUSH_PORT - $WORKER_NUM + 1 ))
  fi

  if [ "$CSS_WORKER_FETCH_PORT" = "" ] || [ "$CSS_WORKER_FETCH_PORT" = "0" ]; then
    FETCH_PORT_FLAG=
    FETCH_PORT_NUM=
  else
    FETCH_PORT_FLAG="--fetchPort"
    FETCH_PORT_NUM=$(( $CSS_WORKER_FETCH_PORT - $WORKER_NUM + 1 ))
  fi

  if [ "$CSS_WORKER_HOST" = "" ]; then
    case `uname` in
        (SunOS)
      CSS_WORKER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
      ;;
        (*)
      CSS_WORKER_HOST="`hostname -f`"
      ;;
    esac
  fi

  "${CSS_HOME}/sbin"/css-daemon.sh start worker $WORKER_NUM $CLASS \
     --host $CSS_WORKER_HOST $PORT_FLAG $PORT_NUM \
     $PUSH_PORT_FLAG $PUSH_PORT_NUM \
     $FETCH_PORT_FLAG $FETCH_PORT_NUM \
     $MASTER "$@"
}

if [ "$CSS_WORKER_INSTANCES" = "" ]; then
  start_instance 1 "$@"
else
  for ((i=0; i<$CSS_WORKER_INSTANCES; i++)); do
    start_instance $(( 1 + $i )) "$@"
  done
fi
