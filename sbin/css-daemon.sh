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

usage="Usage: css-daemon.sh (start|stop|status) (master|worker) <instance-number> <args...>"

# arg must start with:   <main_class> <args...>

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ -z "${CSS_HOME}" ]; then
  export CSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CSS_HOME}/sbin/css-config.sh"


# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi


if [[ -d ${CSS_HOME} ]]; then
  export CLASSPATH=${JAVA_HOME}/lib:${CSS_HOME}/lib/*
fi

echo "use classpath ${CLASSPATH}"
# get arguments

option=$1
shift
instance=$1
shift
instance_num=$1
shift


css_rotate_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
	  num=$2
  fi

  if [ -f "$log" ]; then # rotate logs
	  while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	  done
	  mv "$log" "$log.$num";
  fi
}


if [ "$CSS_IDENT_STRING" = "" ]; then
  export CSS_IDENT_STRING="$USER"
fi

# get log directory
if [ "$CSS_LOG_DIR" = "" ]; then
  export CSS_LOG_DIR="${CSS_HOME}/logs"
fi
mkdir -p "$CSS_LOG_DIR"
touch "$CSS_LOG_DIR"/.css_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$CSS_LOG_DIR"/.css_test
else
  chown "$CSS_IDENT_STRING" "$CSS_LOG_DIR"
fi

if [ "$CSS_PID_DIR" = "" ]; then
  CSS_PID_DIR=/tmp
fi

# some variables
log="$CSS_LOG_DIR/css-$CSS_IDENT_STRING-$instance-$instance_num.out"
pid="$CSS_PID_DIR/css-$CSS_IDENT_STRING-$instance-$instance_num.pid"

export JAVA_OPTS="$JAVA_OPTS -Dcss.log.dir=$CSS_LOG_DIR -Dcss.log.filename=css-$CSS_IDENT_STRING-$instance-$instance_num.out"

# Set default scheduling priority
if [ "$CSS_NICENESS" = "" ]; then
    export CSS_NICENESS=0
fi

execute_command() {
  if [ -z ${CSS_NO_DAEMONIZE+set} ]; then
      nohup -- "$@" >/dev/null 2>&1 &
      newpid="$!"

      echo "$newpid" > "$pid"

      # Poll for up to 5 seconds for the java process to start
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      "$@"
  fi
}

run_command() {
  mode="$1"
  shift

  mkdir -p "$CSS_PID_DIR"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$instance running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  if [ "$CSS_MASTER" != "" ]; then
    echo rsync from "$CSS_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$CSS_MASTER/" "${CSS_HOME}"
  fi

  css_rotate_log "$log"
  echo "starting $instance, logging to $log"

  case "$mode" in
    (class)
      echo "Command: $RUNNER ${JAVA_OPTS} -classpath ${CLASSPATH} $@"
      execute_command nice -n "$CSS_NICENESS" "$RUNNER" ${JAVA_OPTS} -classpath ${CLASSPATH} "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in

  (start)
    run_command class "$@"
    ;;

  (stop)
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $instance process"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $instance to stop"
      fi
    else
      echo "no $instance to stop"
    fi
    ;;

  (status)
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo $instance is running.
        exit 0
      else
        echo $pid file is present but $instance not running
        exit 1
      fi
    else
      echo $instance not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
