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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.WorkerStatus
import com.bytedance.css.common.rpc.RpcEnv
import com.bytedance.css.service.deploy.metadata.WorkerRegistryFactory.{TYPE_STANDALONE, TYPE_ZOOKEEPER}
import com.bytedance.css.service.deploy.metadata.standalone.StandaloneWorkerProvider
import com.bytedance.css.service.deploy.metadata.zookeeper.ZookeeperWorkerProvider
import com.bytedance.css.service.deploy.worker.WorkerInfo

/**
 * hold by master and provides available workers.
 */
trait WorkerProvider extends Logging {

  // backup workers that could be as a supplement if activeWorker is not enough for partition assignment
  // in Standalone mode, this is always empty
  // in ZK mode, it's basically from the entire cluster
  var candidateWorkers: ArrayBuffer[WorkerStatus] = null

  // normal worker management
  val activeWorkers = new ConcurrentHashMap[String, WorkerInfo]()

  def stableAssignableWorkers(): Seq[WorkerInfo] = {
    stableAssignableWorkers {
      workerInfo: WorkerInfo => workerInfo.isActive()
    }
  }

  // used for worker assignment
  // maybe less than activeWorkers basic on the worker load and connection status.
  def stableAssignableWorkers(filter: WorkerInfo => Boolean): Seq[WorkerInfo] = {
    // filter out expired / lost connection / high load workers
    val assignable = activeWorkers.values().asScala
      .filter(filter)
      .toSeq
    logInfo(s"assignableWorkers before:${activeWorkers.size()} after:${assignable.size}")
    assignable.sortBy(_.hostPort)
  }

  def contains(name: String): Boolean = {
    activeWorkers.containsKey(name)
  }

  def get(name: String): WorkerInfo = {
    activeWorkers.get(name)
  }

  def add(workerInfo: WorkerInfo): Unit = {
    activeWorkers.put(workerInfo.name, workerInfo)
  }

  def remove(name: String): WorkerInfo = {
    activeWorkers.remove(name)
  }

  def allocate(target: Int): Unit = {}

  // return worker which need exclude & not in service
  def exclude(): Seq[WorkerInfo] = Seq.empty

  // zk mode does not check timeout worker through heartbeat
  def timeoutWorkers(): Seq[WorkerInfo] = Seq.empty

  def close(): Unit = {}
}

object WorkerProvider {

  def create(cssConf: CssConf, rpcEnv: RpcEnv): WorkerProvider = {
    CssConf.workerRegistryType(cssConf) match {
      case TYPE_STANDALONE => new StandaloneWorkerProvider(cssConf)
      case TYPE_ZOOKEEPER => new ZookeeperWorkerProvider(cssConf, rpcEnv)
    }
  }
}
