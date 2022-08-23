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

CLASS="com.bytedance.css.service.deploy.master.Master"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-master.sh <args...>"
  pattern="Usage:"
  pattern+="\|Using CSS's default log4j profile:"
  pattern+="\|Started daemon with process name"
  pattern+="\|Registered signal handler for"

  "${CSS_HOME}"/sbin/css-daemon.sh start master 1 $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

ORIGINAL_ARGS="$@"

. "${CSS_HOME}/sbin/css-config.sh"

if [ "$MASTER_JAVA_OPTS" != "" ]; then
  JAVA_OPTS=$MASTER_JAVA_OPTS
  export JAVA_OPTS=$JAVA_OPTS
else
  export JAVA_OPTS=""
fi

if [ "$CSS_MASTER_PORT" = "" ]; then
  CSS_MASTER_PORT=9099
fi

if [ "$CSS_MASTER_HOST" = "" ]; then
  case `uname` in
      (SunOS)
	  CSS_MASTER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
	  ;;
      (*)
	  CSS_MASTER_HOST="`hostname -f`"
	  ;;
  esac
fi


"${CSS_HOME}/sbin"/css-daemon.sh start master 1 $CLASS \
  --host $CSS_MASTER_HOST --port $CSS_MASTER_PORT \
  $ORIGINAL_ARGS
