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

# Starts a worker instance on each machine specified in the conf/workers file.

if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CSS_HOME}/sbin/css-config.sh"

# Find the port number for the master
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

# Launch the workers
"${CSS_HOME}/sbin/workers.sh" cd "${CSS_HOME}" \; "${CSS_HOME}/sbin/start-worker.sh" "css://$CSS_MASTER_HOST:$CSS_MASTER_PORT"
