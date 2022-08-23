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

# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   CSS_SLAVES    File naming remote hosts.
#     Default is ${CSS_CONF_DIR}/slaves.
#   CSS_CONF_DIR  Alternate conf dir. Default is ${CSS_HOME}/conf.
#   CSS_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   CSS_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: workers.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CSS_HOME}/sbin/css-config.sh"

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
if [ -f "$CSS_SLAVES" ]; then
  HOSTLIST=`cat "$CSS_SLAVES"`
fi

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export CSS_CONF_DIR="$conf_dir"
  fi
  shift
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$CSS_SLAVES" = "" ]; then
    if [ -f "${CSS_CONF_DIR}/workers" ]; then
      HOSTLIST=`cat "${CSS_CONF_DIR}/workers"`
    else
      HOSTLIST=localhost
    fi
  else
    HOSTLIST=`cat "${CSS_SLAVES}"`
  fi
fi

# By default disable strict host key checking
if [ "$CSS_SSH_OPTS" = "" ]; then
  CSS_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for worker in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${CSS_SSH_FOREGROUND}" ]; then
    ssh $CSS_SSH_OPTS "$worker" $"${@// /\\ }" \
      2>&1 | sed "s/^/$worker: /"
  else
    ssh $CSS_SSH_OPTS "$worker" $"${@// /\\ }" \
      2>&1 | sed "s/^/$worker: /" &
  fi
  if [ "$CSS_SLAVE_SLEEP" != "" ]; then
    sleep $CSS_SLAVE_SLEEP
  fi
done

wait
