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
import com.bytedance.css.common.protocol.CssRpcMessage.UnregisterShuffle
import com.bytedance.css.common.rpc.RpcEndpointRef
import com.bytedance.css.common.util.ThreadUtils
import com.bytedance.css.service.deploy.common.ScheduledManager
import com.bytedance.css.service.deploy.metadata.ExternalShuffleMeta

class ShuffleAppManager(
    cssConf: CssConf,
    val shuffleStageManager: ShuffleStageManager,
    shuffleWorkerManager: ShuffleWorkerManager,
    externalShuffleMeta: ExternalShuffleMeta,
    masterRpcEndpointRef: RpcEndpointRef) extends Logging {

  private val appTimeoutMs = CssConf.appTimeoutMs(cssConf)
  private val appLastHeartbeat = new ConcurrentHashMap[String, Long]()

  private val asyncThreadPool = ThreadUtils
    .newDaemonCachedThreadPool("ThreadPool for time consuming operations", 16)
  val scheduledManager = new ScheduledManager("app-forward-message-thread", 1)
  scheduledManager.addScheduledTask("appTimeOutTask", checkApplicationTimeOut, 0, appTimeoutMs / 2)

  def handleApplicationHeartbeat(appId: String): Unit = {
    if (!appLastHeartbeat.containsKey(appId)) {
      externalShuffleMeta.appCreated(appId)
    }
    appLastHeartbeat.put(appId, System.currentTimeMillis())
  }

  def start(): Unit = {
    scheduledManager.start()
  }

  def stop(): Unit = {
    externalShuffleMeta.cleanupIfNeeded()
    asyncThreadPool.shutdown()
    scheduledManager.stop()
  }

  private def checkApplicationTimeOut(): Unit = {
    val appLostSet = new util.HashSet[String]()
    val currentTime = System.currentTimeMillis()
    logDebug(s"Check for application timeout with $currentTime")
    val keys = appLastHeartbeat.keySet().asScala.toList
    keys.foreach { appId =>
      if (appLastHeartbeat.get(appId) < currentTime - appTimeoutMs) {
        logWarning(s"Application $appId timeout, trigger ApplicationLost")
        handleApplicationLost(appId)
        appLastHeartbeat.remove(appId)
        // mark appId to remove
        appLostSet.add(appId)
      }
    }
    shuffleWorkerManager.getActiveWorkers().foreach { worker =>
      worker.appFinishSet.addAll(appLostSet)
    }
  }

  private def handleApplicationLost(appId: String) : Unit = {
    asyncThreadPool.submit(new Runnable {
      override def run(): Unit = {
        val expiredShuffles = shuffleStageManager.getAppShuffle(appId)
        expiredShuffles.foreach { key =>
          val splits = key.split("-")
          val appId = splits.dropRight(1).mkString("-")
          val shuffleId = splits.last.toInt
          masterRpcEndpointRef.ask(UnregisterShuffle(appId, shuffleId))
        }
      }
    })
    externalShuffleMeta.appRemoved(appId)
  }
}
