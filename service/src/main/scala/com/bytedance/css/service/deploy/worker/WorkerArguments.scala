/*
 * Copyright 2022 Bytedance Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.css.service.deploy.worker

import scala.annotation.tailrec

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.util.{IntParam, Utils}

class WorkerArguments(args: Array[String], conf: CssConf) {

  var bindHost = "0.0.0.0"
  var host = Utils.localHostName()
  // for rpc control message port
  // pushPort and fetchPort should be configured at CssConf
  var port = 0

  var pushPort = -1
  var fetchPort = -1

  // default css master url for workers to register
  var master: String = s"css://$host:9099"
  var propertiesFile: String = null

  parse(args.toList)

  propertiesFile = Utils.loadDefaultCssProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--bindHost" | "-bh") :: value :: tail =>
      Utils.checkHost(value)
      bindHost = value
      parse(tail)

    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--pushPort" | "-pp") :: IntParam(value) :: tail =>
      pushPort = value
      parse(tail)

    case ("--fetchPort" | "-fp") :: IntParam(value) :: tail =>
      fetchPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--conf") :: confKey :: confValue :: tail =>
      conf.set(confKey, confValue)
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      master = value
      conf.set("css.master.address", master)
      parse(tail)

    case Nil =>
      if (master == null) {  // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      "Usage: Worker [options] <master>\n" +
        "\n" +
        "Master must be a URL of the form css://hostname:port\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST                   Hostname to listen on\n" +
        "  -p PORT, --port PORT                   RpcPort to listen on (default: random)\n" +
        "  -pp PUSHPORT, --pushPort PUSHPORT      PushPort to listen on (default: random)\n" +
        "  -fp FETCHPORT, --fetchPort FETCHPORT   FetchPort to listen on (default: random)\n" +
        "  --properties-file FILE                 Path to a custom CSS properties file.\n" +
        "                                         Default is conf/css-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
