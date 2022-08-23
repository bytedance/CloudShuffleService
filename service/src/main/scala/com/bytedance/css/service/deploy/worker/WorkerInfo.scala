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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.{PartitionInfo, WorkerStatus}
import com.bytedance.css.common.rpc.RpcEndpointRef
import com.bytedance.css.common.rpc.netty.NettyRpcEndpointRef
import com.bytedance.css.common.util.Utils
import io.netty.util.internal.ConcurrentSet

class WorkerInfo(
    val name: String,                  // unique name for this worker
    val host: String,                  // hostname for this worker
    val rpcPort: Int,                  // rpc port for control message
    val pushPort: Int,                 // rpc port for push data call
    val fetchPort: Int,                // rpc port for fetch data call
    var workerRpcRef: RpcEndpointRef)  // rpc ref for sending rpc to this worker, for control message flow
  extends Serializable with Logging {

  def this(
      host: String,                  // hostname for this worker
      rpcPort: Int,                  // rpc port for control message
      pushPort: Int,                 // rpc port for push data call
      fetchPort: Int,                // rpc port for fetch data call
      workerRpcRef: RpcEndpointRef) = {
    this(host, host, rpcPort, pushPort, fetchPort, workerRpcRef)
  }

  var rttAvgStat: Long = 0L

  Utils.checkHost(host)
  assert(rpcPort > 0)
  assert(pushPort > 0)
  assert(fetchPort > 0)

  // mark current app has lost or finish
  val appFinishSet = new ConcurrentSet[String]()

  // heartbeat timestamp that kept and updated in master node to determined whether the target worker is dead
  var lastHeartbeat: Long = System.currentTimeMillis()

  protected val partitionMapV2 = new ConcurrentHashMap[String, PartitionInfo]()
  protected val shuffleKeySetV2 = new ConcurrentSet[String]()

  // TODO current conditions are always true
  // rpcEndpointRef with client args will always be null. because this.client will be set by server side.
  def isActive(): Boolean = {
    workerRpcRef != null || workerRpcRef.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  def hostPort: String = {
    host + ":" + rpcPort
  }

  def shuffleKeySet(): util.HashSet[String] = {
    new util.HashSet[String](shuffleKeySetV2)
  }

  def getAllShufflePartitions(shuffleKey: String): util.List[PartitionInfo] = {
    if (shuffleKeySetV2.contains(shuffleKey)) {
      val allEpochKeys = partitionMapV2.keySet().asScala.filter(k => k.startsWith(s"${shuffleKey}-"))
      allEpochKeys.map(k => partitionMapV2.get(k)).toList.asJava
    } else {
      List.empty[PartitionInfo].asJava
    }
  }

  def getAllShuffleKeyByAppId(appId: String): util.List[String] = {
    shuffleKeySetV2.asScala.filter(_.startsWith(s"${appId}-")).toList.asJava
  }

  def addShufflePartition(shuffleKey: String, partitionInfo: PartitionInfo): Unit = {
    if (partitionInfo != null) {
      shuffleKeySetV2.add(shuffleKey)
      val epochKey = Utils.getEpochKeyWithShuffleKey(shuffleKey, partitionInfo.getReducerId, partitionInfo.getEpochId)
      partitionMapV2.putIfAbsent(epochKey, partitionInfo)
    }
  }

  def getShufflePartition(shuffleKey: String, reducerId: Int, epochId: Int): PartitionInfo = {
    val epochKey = Utils.getEpochKeyWithShuffleKey(shuffleKey, reducerId, epochId)
    partitionMapV2.get(epochKey)
  }

  def removeShufflePartitions(shuffleKey: String): Unit = {
    if (shuffleKeySetV2.contains(shuffleKey)) {
      val allEpochKeys = partitionMapV2.keySet().asScala.filter(k => k.startsWith(s"${shuffleKey}-"))
      allEpochKeys.foreach(k => partitionMapV2.remove(k))
      shuffleKeySetV2.remove(shuffleKey)
    }
  }

  def containsShuffle(shuffleKey: String): Boolean = {
    shuffleKeySetV2.contains(shuffleKey)
  }

  override def toString(): String = {
    s"""
       |Name: $name
       |Address: $hostPort
       |PushPort: $pushPort
       |FetchPort: $fetchPort
     """.stripMargin
  }

  override def equals(obj: Any): Boolean = if (!obj.isInstanceOf[WorkerInfo]) {
    false
  } else {
    name.equals(obj.asInstanceOf[WorkerInfo].name)
  }

  override def hashCode(): Int = {
    name.hashCode()
  }
}

object WorkerInfo {
  def toWorkerStatus(workerInfo: WorkerInfo, rttAvgStat: Long): WorkerStatus = {
    new WorkerStatus(
      workerInfo.name,
      workerInfo.host,
      workerInfo.rpcPort,
      workerInfo.pushPort,
      workerInfo.fetchPort,
      rttAvgStat,
      System.currentTimeMillis())
  }

  def fromWorkerStatus(status: WorkerStatus, workerRpcRef: RpcEndpointRef): WorkerInfo = {
    new WorkerInfo(status.name, status.host, status.rpcPort, status.pushPort, status.fetchPort, workerRpcRef)
  }
}
