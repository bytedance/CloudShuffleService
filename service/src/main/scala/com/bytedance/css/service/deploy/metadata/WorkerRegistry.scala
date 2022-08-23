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

package com.bytedance.css.service.deploy.metadata

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.rpc.RpcEnv
import com.bytedance.css.service.deploy.metadata.standalone.StandaloneWorkerRegistry
import com.bytedance.css.service.deploy.metadata.zookeeper.ZooKeeperWorkerRegistry
import com.bytedance.css.service.deploy.worker.WorkerInfo
import com.bytedance.css.service.deploy.worker.handler.RecycleShuffleHandler

/**
 * hold by worker, registers with the master and provides its own node information.
 */
trait WorkerRegistry {

  def register(workerInfo: WorkerInfo): Unit

  def update(workerInfo: WorkerInfo, rttAvgStat: Long): Unit

  def close(): Unit
}

object WorkerRegistryFactory {

  val TYPE_ZOOKEEPER = "zookeeper"
  val TYPE_STANDALONE = "standalone"

  def create(
    rpcEnv: RpcEnv,
    cssConf: CssConf,
    handler: RecycleShuffleHandler): WorkerRegistry = {
    CssConf.workerRegistryType(cssConf) match {
      case TYPE_STANDALONE => new StandaloneWorkerRegistry(rpcEnv, cssConf, handler)
      case TYPE_ZOOKEEPER => new ZooKeeperWorkerRegistry(rpcEnv, cssConf, handler)
    }
  }
}
