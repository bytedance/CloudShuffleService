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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.WorkerAddress
import com.bytedance.css.common.rpc.RpcEnv
import com.bytedance.css.common.util.Collections._
import com.bytedance.css.service.deploy.common.ScheduledManager
import com.bytedance.css.service.deploy.metadata.WorkerProvider
import com.bytedance.css.service.deploy.worker.WorkerInfo
import io.netty.util.internal.ConcurrentSet


class ShuffleWorkerManager(cssConf: CssConf, rpcEnv: RpcEnv) extends Logging {

  // key appId-shuffleId
  // store all relative shuffle worker (host:rpcPort) for a shuffleKey
  private[deploy] val shuffleWorkerSetMap = new ConcurrentHashMap[String, ConcurrentSet[String]]()

  private val workerLostMap = new ConcurrentHashMap[String, WorkerInfo]()

  private val workerTimeoutMs = CssConf.workerTimeoutMs(cssConf)
  val scheduledManager = new ScheduledManager("worker-forward-message-thread", 1)
  scheduledManager.addScheduledTask("workerTimeOutTask", checkWorkerTimeout, 0, workerTimeoutMs)

  val workerProvider = WorkerProvider.create(cssConf, rpcEnv)

  def handleRegisterWorker(name: String, workerInfo: WorkerInfo): Boolean = {
    logInfo(s"Registering worker(${workerInfo.name}) " +
      s"${workerInfo.host}:${workerInfo.rpcPort}:${workerInfo.pushPort}:${workerInfo.fetchPort} .")
    var registerStatus = false
    if (workerProvider.contains(name) || workerLostMap.containsKey(name)) {
      workerLostMap.synchronized {
        if (!workerLostMap.containsKey(name)) {
          workerLostMap.put(name, workerInfo)
        }
      }
      workerProvider.remove(name)
      logWarning(s"Worker ${name} re-register, might be WorkerLost. ignored register request.")
    } else {
      workerProvider.add(workerInfo)
      logInfo(s"Registered worker $workerInfo")
      registerStatus = true
    }
    registerStatus
  }

  def handleWorkerHeartBeat(
      name: String,
      rttAvgStat: Long,
      shuffleKeys: util.HashSet[String]): Boolean = {
    logDebug(s"Received heartbeat from worker ${name}")
    if (!workerProvider.contains(name)) {
      logInfo(s"Received heartbeat from unknown worker ${name}")
      return false
    }

    val worker = workerProvider.get(name)
    worker.synchronized {
      worker.lastHeartbeat = System.currentTimeMillis()
    }
    return true
  }

  private def checkWorkerTimeout() {
    workerProvider.timeoutWorkers().foreach(lostWorker => {
      logWarning(s"Worker ${lostWorker.name} timeout! Trigger WorkerLost event.")
      if (!workerLostMap.containsKey(lostWorker.name)) {
        workerLostMap.put(lostWorker.name, lostWorker)
      } else {
        workerLostMap.remove(lostWorker.name)
      }
      workerProvider.remove(lostWorker.name)
    })
  }

  def addShuffleStageWorker(
      shuffleKey: String,
      assignWorkers: List[WorkerAddress]): Unit = {
    val relativeWorkers = getStableAssignableWorkers().filter(w => {
      assignWorkers.exists(p => (w.host.equals(p.host) && w.pushPort == p.port))
    }).map(_.name)

    shuffleWorkerSetMap.computeWhenAbsent(shuffleKey, _ => { new ConcurrentSet[String]() })
      .addAll(relativeWorkers.asJava)
  }

  def removeShuffleStageWorker(shuffleKey: String): Unit = {
    shuffleWorkerSetMap.remove(shuffleKey)
  }

  def getShuffleStageWorker(shuffleKey: String): Seq[WorkerInfo] = {
    val shuffleWorker = shuffleWorkerSetMap.get(shuffleKey).asScala
      .map(workerProvider.get).filter(_ != null).toList
    return shuffleWorker
  }

  def getStableAssignableWorkers(): Seq[WorkerInfo] = {
    workerProvider.stableAssignableWorkers
  }

  def getActiveWorkers(): Seq[WorkerInfo] = {
    workerProvider.activeWorkers.values().asScala.toSeq
  }

  def start(): Unit = {
    scheduledManager.start()
  }

  def stop(): Unit = {
    workerProvider.close()
    scheduledManager.stop()
  }
}
