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

package com.bytedance.css.impl

import java.util

import scala.collection.JavaConverters._

import com.bytedance.css.api.CssShuffleContext
import com.bytedance.css.common.CssConf
import com.bytedance.css.service.deploy.master.Master

class CssShuffleContextImpl extends CssShuffleContext {

  private var master: Master = null
  private val cssConf: CssConf = new CssConf()

  override def startMaster(host: String, port: Int, confMap: util.Map[String, String]): Unit = {
    if (master == null) {
      this.synchronized {
        if (master == null) {
          confMap.asScala.foreach(pair => cssConf.set(pair._1, pair._2))
          master = Master.getOrCreate(host, port, cssConf)
        }
      }
    }
  }

  override def stopMaster(): Unit = {
    if (master != null) {
      master.stop()
    }
  }

  override def getMasterHost: String = {
    if (master == null) {
      throw new Exception("Css Master is not created yet.")
    } else {
      master.rpcEnv.address.host
    }
  }

  override def getMasterPort: Int = {
    if (master == null) {
      throw new Exception("Css Master is not created yet.")
    } else {
      master.rpcEnv.address.port
    }
  }

  override def allocateWorkerIfNeeded(numWorkers: Int): Unit = {
    if (master == null) {
      throw new Exception("Css Master is not created yet.")
    } else {
      // async allocate numWorkers for current application
      val actualNumWorkers = Math.min(
        Math.max(
          2,
          (numWorkers * CssConf.workerAllocateExtraRatio(cssConf)).toInt),
        CssConf.maxAllocateWorker(cssConf))
      master.getShuffleWorkerManager().workerProvider.allocate(actualNumWorkers)
    }
  }

  override def waitUntilShuffleCommitted(appId: String, shuffleId: Int): Unit = {
    if (master == null) {
      throw new Exception("Css Master is not created yet.")
    } else {
      // sync await until shuffle stage ended.
      master.getShuffleStageManager().finishShuffleStage(appId, shuffleId, true)
    }
  }

  override def eagerDestroyShuffle(appId: String, shuffleId: Int): Unit = {
    // TODO this API is mainly for shuffle re-computation
    // we need to destroy current shuffle meta and data, before we can actually retry shuffle.
  }
}
