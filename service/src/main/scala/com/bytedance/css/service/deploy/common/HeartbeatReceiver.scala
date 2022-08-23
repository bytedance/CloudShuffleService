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

package com.bytedance.css.service.deploy.common

import java.util

import scala.collection.JavaConverters._

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.protocol.CssStatusCode
import com.bytedance.css.common.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import com.bytedance.css.service.deploy.master.{Master}
import com.bytedance.css.service.deploy.worker.WorkerInfo

class HeartbeatReceiver(
    val rpcEnv: RpcEnv,
    master: Master) extends ThreadSafeRpcEndpoint with Logging {

  private lazy val shuffleAppManager = master.getShuffleAppManager()
  private lazy val shuffleWorkerManager = master.getShuffleWorkerManager()

  override def onStart(): Unit = {
  }

  override def receive: PartialFunction[Any, Unit] = {
    case HeartbeatFromApp(appId) =>
      shuffleAppManager.handleApplicationHeartbeat(appId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req: RegisterWorker =>
      handleRegisterWorker(context, req: RegisterWorker)

    case req: HeartbeatFromWorker =>
      handleHeartBeatFromWorker(context, req: HeartbeatFromWorker)
  }

  def handleRegisterWorker(
      context: RpcCallContext,
      req: RegisterWorker): Unit = {
    val workerInfo = new WorkerInfo(req.name, req.host, req.rpcPort, req.pushPort, req.fetchPort, req.workerRef)
    val registerStatus = shuffleWorkerManager.handleRegisterWorker(workerInfo.name, workerInfo)
    if (!registerStatus) {
      context.reply(RegisterWorkerResponse(CssStatusCode.Failed))
    } else {
      context.reply(RegisterWorkerResponse(CssStatusCode.Success))
    }
  }

  def handleHeartBeatFromWorker(
      context: RpcCallContext,
      req: HeartbeatFromWorker): Unit = {
    val handled = shuffleWorkerManager.handleWorkerHeartBeat(req.name, req.rttAvgStat, req.shuffleKeys)
    val expiredShuffleKeys = new util.HashSet[String]
    val expiredAppIds = new util.HashSet[String]()
    if (handled) {
      req.shuffleKeys.asScala.foreach { shuffleKey =>
        val splits = shuffleKey.split("-")
        val appId = splits.dropRight(1).mkString("-")
        val shuffleId = splits.last.toInt
        if (!shuffleAppManager.shuffleStageManager.validateRegisterShuffle(appId, shuffleId)) {
          expiredShuffleKeys.add(shuffleKey)
        }
      }
      // appId must not exist in all shuffle relate event and clear after expired shuffle keys for app clear
      val worker = shuffleWorkerManager.workerProvider.get(req.name)
      if (!worker.appFinishSet.isEmpty) {
        worker.appFinishSet.asScala.foreach { appId =>
          if (shuffleAppManager.shuffleStageManager.getAppShuffle(appId).isEmpty &&
            !expiredShuffleKeys.asScala.exists(_.startsWith(appId))) {
            expiredAppIds.add(appId)
          }
        }
        worker.appFinishSet.removeAll(expiredAppIds)
      }
    }
    context.reply(HeartbeatResponse(expiredShuffleKeys, expiredAppIds))
  }

  override def onStop(): Unit = {
  }
}
