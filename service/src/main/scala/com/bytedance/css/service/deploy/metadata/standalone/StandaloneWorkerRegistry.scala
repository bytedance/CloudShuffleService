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

package com.bytedance.css.service.deploy.metadata.standalone

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.{CssStatusCode, RpcNameConstants}
import com.bytedance.css.common.protocol.CssRpcMessage.{HeartbeatFromWorker, HeartbeatResponse, RegisterWorker, RegisterWorkerResponse}
import com.bytedance.css.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import com.bytedance.css.common.util.Utils
import com.bytedance.css.service.deploy.metadata.WorkerRegistry
import com.bytedance.css.service.deploy.worker.WorkerInfo
import com.bytedance.css.service.deploy.worker.handler.RecycleShuffleHandler

class StandaloneWorkerRegistry(
    val rpcEnv: RpcEnv,
    val cssConf: CssConf,
    val handler: RecycleShuffleHandler) extends WorkerRegistry with Logging {

  val heartbeatRef: RpcEndpointRef =
    rpcEnv.setupEndpointRef(RpcAddress.fromCssURL(CssConf.masterAddress(cssConf)), RpcNameConstants.HEARTBEAT)

  override def register(workerInfo: WorkerInfo): Unit = {
    val req = RegisterWorker(
      workerInfo.name, workerInfo.host, workerInfo.rpcPort,
      workerInfo.pushPort, workerInfo.fetchPort, workerInfo.workerRpcRef)

    var res = heartbeatRef.askSync[RegisterWorkerResponse](req)
    var registerTimeoutMs = CssConf.workerRegisterTimeoutMs(cssConf)
    val sleepInterval = 2000
    while (!res.statusCode.equals(CssStatusCode.Success) && registerTimeoutMs > 0) {
      logWarning(s"Register worker failed with StatusCode: ${res.statusCode}")
      Thread.sleep(sleepInterval)
      registerTimeoutMs = registerTimeoutMs - sleepInterval
      logInfo("Trying to re-register with master.")
      res = heartbeatRef.askSync[RegisterWorkerResponse](req)
    }

    if (!res.statusCode.equals(CssStatusCode.Success)) {
      logError(s"Failed to register worker within ${CssConf.workerRegisterTimeoutMs(cssConf)}ms")
      System.exit(-1)
    }
  }

  override def update(workerInfo: WorkerInfo, rttAvgStat: Long): Unit = {
    Utils.tryLogNonFatalError {
      val res = heartbeatRef.askSync[HeartbeatResponse](
        HeartbeatFromWorker(workerInfo.name, rttAvgStat, workerInfo.shuffleKeySet()))
      if (!res.expiredShuffleKeys.isEmpty) {
        logInfo(s"Told from Standalone Master to cleanup shuffle files " +
          s"${res.expiredShuffleKeys.asScala.mkString(",")}")
        handler.recycleShuffle(res.expiredShuffleKeys)
      }
      if (!res.expiredAppIds.isEmpty) {
        logInfo(s"Told from Standalone Master to cleanup expired appId dirs " +
          s"${res.expiredAppIds.asScala.mkString(",")}")
        handler.recycleApplication(res.expiredAppIds)
      }
    }
  }

  override def close(): Unit = {}
}
