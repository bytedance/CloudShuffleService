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

package com.bytedance.css.service.deploy.metadata.zookeeper

import java.util.Comparator

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.{RpcNameConstants, WorkerStatus}
import com.bytedance.css.common.rpc.{RpcAddress, RpcEnv}
import com.bytedance.css.common.util.{JsonUtils, ThreadUtils}
import com.bytedance.css.service.deploy.metadata.WorkerProvider
import com.bytedance.css.service.deploy.worker.WorkerInfo

class ZookeeperWorkerProvider(cssConf: CssConf, rpcEnv: RpcEnv) extends WorkerProvider {

  private val zkClient = ZookeeperClient.build(cssConf)
  private val zkMaxThreads = CssConf.zkMaxParallelism(zkClient.cssConf)
  private val workerTimeoutMs = CssConf.workerTimeoutMs(zkClient.cssConf)
  @volatile private var listFromZk: Boolean = false
  private var zkWorkerNames: Seq[String] = null

  override def allocate(target: Int): Unit = synchronized {
    val allocateStartMs = System.currentTimeMillis()

    // list from zk to get all candidate worker will only execute once
    if (!listFromZk) {
      val startMs = System.currentTimeMillis()
      zkWorkerNames = zkClient.list(zkClient.workersPath)
      listFromZk = true
      logInfo(s"listFromZk used ${System.currentTimeMillis() - startMs}ms with ${zkWorkerNames.size} workers.")
    }

    // get worker status for candidateWorkers
    candidateWorkers = ThreadUtils.parmap(
      zkWorkerNames, "candidateWorkers", zkMaxThreads) { name =>
      try {
        val startMs = System.currentTimeMillis()
        val json = zkClient.getData(zkClient.getWorkPath(name))
        val status = JsonUtils.deserialize(json, classOf[WorkerStatus])
        if (status.lastHeartbeat + workerTimeoutMs > startMs) {
          status
        } else {
          null
        }
      } catch {
        case ex: Exception =>
          logError(s"candidateWorkers WorkerStatus deserialize failed with $name", ex)
          null
      }
    }.to[mutable.ArrayBuffer].filter(_ != null)
    logInfo(s"candidateWorkers after heartbeat filtered, origin: " +
      s"${zkWorkerNames.size}, current: ${candidateWorkers.size}")

    // sorted in ascending order of rttAvgStat, the top worker node is the node with relatively idle load
    candidateWorkers.asJava.sort(new Comparator[WorkerStatus] {
      override def compare(o1: WorkerStatus, o2: WorkerStatus): Int = {
        o1.rttAvgStat.compareTo(o2.rttAvgStat)
      }
    })

    candidateWorkers.foreach(w => logInfo(s"${w.host} ${w.rttAvgStat}ms"))

    val actualTarget = Math.min(target, candidateWorkers.size)
    logInfo(s"target: $target, actualTarget: $actualTarget, candidateWorkers: ${candidateWorkers.size}")

    val newWorkers: Seq[WorkerStatus] = candidateWorkers.take(actualTarget)
    candidateWorkers = candidateWorkers.drop(actualTarget)

    val startMs = System.currentTimeMillis()
    ThreadUtils.parmap(newWorkers, "parallelism-init", zkMaxThreads) { status =>
      try {
        val workerInfo = WorkerInfo.fromWorkerStatus(
          status,
          rpcEnv.setupEndpointRef(
            RpcAddress(status.host, status.rpcPort), RpcNameConstants.WORKER_EP)
        )
        workerInfo.rttAvgStat = status.rttAvgStat
        activeWorkers.put(status.name, workerInfo)
      } catch {
        case ex: Exception =>
          logError(s"WorkerStatus to WorkerInfo failed with $status", ex)
      }
    }
    logInfo(s"current: ${activeWorkers.size()}, target: $target, actualTarget: $actualTarget, " +
      s"worker ctor used ${System.currentTimeMillis() - startMs}ms " +
      s"entire allocate used ${System.currentTimeMillis() - allocateStartMs}ms")
  }

  override def close(): Unit = {
    zkClient.close()
  }
}
