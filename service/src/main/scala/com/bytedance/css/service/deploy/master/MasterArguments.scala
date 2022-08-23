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

package com.bytedance.css.service.deploy.master

import scala.annotation.tailrec

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.util.{IntParam, Utils}

class MasterArguments(args: Array[String], conf: CssConf) {

  var host = Utils.localHostName()
  var port = 9099
  var propertiesFile: String = null

  if (System.getenv("CSS_MASTER_HOST") != null) {
    host = System.getenv("CSS_MASTER_HOST")
  }
  if (System.getenv("CSS_MASTER_PORT") != null) {
    port = System.getenv("CSS_MASTER_PORT").toInt
  }

  parse(args.toList)

  propertiesFile = Utils.loadDefaultCssProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--local-mode") :: tail =>
      conf.set("css.local.mode", "true")
      parse(tail)

    case ("--conf") :: confKey :: confValue :: tail =>
      conf.set(confKey, confValue)
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => // No-op

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  private def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      "Usage: Master [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST        Hostname to listen on\n" +
        "  -p PORT, --port PORT        Port to listen on (default: 9099)\n" +
        "  --properties-file FILE      Path to a custom CSS properties file.\n" +
        "                              Default is conf/css-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
