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

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.metrics.MetricsSystem
import com.bytedance.css.common.protocol._
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.rpc._
import com.bytedance.css.common.rpc.netty.{NettyRpcEnv, RemoteNettyRpcCallContext}
import com.bytedance.css.common.util.Utils
import com.bytedance.css.service.deploy.common.HeartbeatReceiver
import com.bytedance.css.service.deploy.metadata.ExternalShuffleMeta
import org.apache.hadoop.util.ShutdownHookManager

class Master(
    override val rpcEnv: RpcEnv,
    val conf: CssConf)
  extends RpcEndpoint with Logging {

  private[deploy] val reducerFileGroupsReuseByteBufferCache =
    new ConcurrentHashMap[String, ByteBuffer]()

  private lazy val assignStrategy = AssignStrategy.buildAssignStrategy(conf)
  private lazy val externalShuffleMeta = ExternalShuffleMeta.create(conf)
  private lazy val shuffleTaskManager = new ShuffleTaskManager
  private lazy val shuffleWorkerManager = new ShuffleWorkerManager(conf, rpcEnv)
  private lazy val shuffleStageManager = new ShuffleStageManagerImpl(
    conf, assignStrategy, shuffleTaskManager, shuffleWorkerManager, externalShuffleMeta)
  private lazy val shuffleAppManager = new ShuffleAppManager(
    conf, shuffleStageManager, shuffleWorkerManager, externalShuffleMeta, self)

  private lazy val metricsSystem = MetricsSystem.createMetricsSystem(MetricsSystem.MASTER, conf)
  private lazy val masterSource = MasterSource.create(CssConf.clusterName(conf), rpcEnv.address.host)

  override def onStart(): Unit = {
    logInfo(s"Master onStart called.")
    shuffleAppManager.start()
    shuffleStageManager.start()
    shuffleWorkerManager.start()

    metricsSystem.registerSource(masterSource)
    metricsSystem.start()
  }

  override def onStop(): Unit = {
    logInfo(s"Master onStop called.")
    metricsSystem.report()
    shuffleAppManager.stop()
    shuffleStageManager.stop()
    shuffleWorkerManager.stop()
    metricsSystem.stop()
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"Client $address got disassociated.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case req: StageEnd =>
      val startMs = System.currentTimeMillis()
      masterSource.withEventMetrics("StageEnd") {
        handleStageEnd(null, req)
      }
      logInfo(s"handleStageEnd for ${req.applicationId}-${req.shuffleId} " +
        s"take ${System.currentTimeMillis() - startMs} ms.")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req: RegisterPartitionGroup =>
      masterSource.withEventMetrics("RegisterPartitionGroup") {
        handleRegisterPartitionGroup(context, req)
      }

    case req: ReallocatePartitionGroup =>
      masterSource.withEventMetrics("ReallocatePartitionGroup") {
        handleReallocatePartitionGroup(context, req)
      }

    case req: MapperEnd =>
      masterSource.withEventMetrics("MapperEnd") {
        handleMapperEnd(context, req)
      }

    case req: GetReducerFileGroups =>
      masterSource.withEventMetrics("GetReducerFileGroups") {
        handleGetReducerFileGroups(context, req)
      }

    case req: UnregisterShuffle =>
      handleUnregisterShuffle(context, req)
  }

  def handleRegisterPartitionGroup(
      context: RpcCallContext,
      req: RegisterPartitionGroup): Unit = {

    val registeredPG = shuffleStageManager.registerShuffleStage(
      req.applicationId,
      req.shuffleId,
      req.numMappers,
      req.numPartitions,
      req.maxPartitionsPerGroup
    )
    context.reply(RegisterPartitionGroupResponse(CssStatusCode.Success, registeredPG))
  }

  def handleReallocatePartitionGroup(
      context: RpcCallContext,
      req: ReallocatePartitionGroup): Unit = {

    val shuffleKey = Utils.getShuffleKey(req.applicationId, req.shuffleId)

    val validRegistered = shuffleStageManager.validateRegisterShuffle(req.applicationId, req.shuffleId)
    if (!validRegistered) {
      logError(s"[handleReallocatePartitionGroup] shuffle $shuffleKey not registered!")
      context.reply(ReallocatePartitionGroupResponse(CssStatusCode.ShuffleNotRegistered, null))
      return
    }

    val validMapperEnded = shuffleTaskManager.validateShuffleTaskEnded(shuffleKey, req.mapId)
    if (validMapperEnded) {
      logError(s"[handleReallocatePartitionGroup] shuffle $shuffleKey mapper ended!")
      context.reply(ReallocatePartitionGroupResponse(CssStatusCode.MapEnded, null))
      return
    }

    val reallocatePG = shuffleStageManager.reallocateShufflePartition(
      req.applicationId,
      req.shuffleId,
      req.mapId,
      req.attemptId,
      req.oldPartitionGroup
    )

    context.reply(ReallocatePartitionGroupResponse(CssStatusCode.Success, reallocatePG))
  }

  def handleMapperEnd(
      context: RpcCallContext,
      req: MapperEnd): Unit = {

    val shuffleKey = Utils.getShuffleKey(req.applicationId, req.shuffleId)

    shuffleTaskManager.shuffleTaskEnded(
      shuffleKey,
      req.mapId,
      req.attemptId,
      req.numMappers,
      req.epochList,
      req.batchBlacklist
    )

    val triggerStageEnd = shuffleTaskManager.validateEffectiveTaskAttempt(shuffleKey, req.mapId, req.attemptId) &
      shuffleTaskManager.validateAllTaskEnded(shuffleKey)

    if (triggerStageEnd) {
      self.send(StageEnd(req.applicationId, req.shuffleId))
    }

    // reply success
    context.reply(MapperEndResponse(CssStatusCode.Success))
  }

  def handleStageEnd(
      context: RpcCallContext,
      req: StageEnd): Unit = {

    val shuffleKey = Utils.getShuffleKey(req.applicationId, req.shuffleId)
    val validRegistered = shuffleStageManager.validateRegisterShuffle(req.applicationId, req.shuffleId)
    if (!validRegistered) {
      logWarning(s"handleStageEnd for non-register-shuffle $shuffleKey.")
      shuffleStageManager.commitShuffleStage(req.applicationId, req.shuffleId, true)
      if (context != null) {
        context.reply(StageEndResponse(CssStatusCode.ShuffleNotRegistered))
      }
      return
    }
    val dataLost = shuffleStageManager.commitShuffleStage(req.applicationId, req.shuffleId, false)
    if (dataLost) {
      if (context != null) {
        context.reply(StageEndResponse(CssStatusCode.Failed))
      }
    } else {
      if (context != null) {
        context.reply(StageEndResponse(CssStatusCode.Success))
      }
    }
  }

  def handleGetReducerFileGroups(
      context: RpcCallContext,
      req: GetReducerFileGroups): Unit = {
    val shuffleKey = Utils.getShuffleKey(req.applicationId, req.shuffleId)
    var status: CssStatusCode = null
    val stageStatus = shuffleStageManager.finishShuffleStage(req.applicationId, req.shuffleId)
    stageStatus match {
      case NoStageEndStatus => status = CssStatusCode.Failed
      case StageEndTimeOutStatus => status = CssStatusCode.Timeout
      case StageEndRunningStatus => status = CssStatusCode.Waiting
      case StageEndDataLostStatus => status = CssStatusCode.StageEndDataLost
      case StageEndFinishStatus => status = {
        if (!reducerFileGroupsReuseByteBufferCache.containsKey(shuffleKey)) {
          reducerFileGroupsReuseByteBufferCache.synchronized {
            if (!reducerFileGroupsReuseByteBufferCache.containsKey(shuffleKey)) {
              val stageResult = shuffleStageManager.getShuffleStageResult(req.applicationId, req.shuffleId)
              val response = GetReducerFileGroupsResponse(
                CssStatusCode.Success,
                stageResult.shuffleFileGroup,
                stageResult.mapperAttempts,
                stageResult.failedPartitionBatches
              )
              val byteBuffer = rpcEnv.asInstanceOf[NettyRpcEnv].serialize(response)
              reducerFileGroupsReuseByteBufferCache.put(shuffleKey, byteBuffer)
              logInfo(s"reduce file groups cache buffer with ${shuffleKey} size ${byteBuffer.remaining()}")
            }
          }
        }
        CssStatusCode.Success
      }
    }
    if (status == CssStatusCode.Success) {
      // ByteBuffer reused
      if (context.isInstanceOf[RemoteNettyRpcCallContext]) {
        context.reply(reducerFileGroupsReuseByteBufferCache.get(shuffleKey).duplicate())
      } else {
        val stageResult = shuffleStageManager.getShuffleStageResult(req.applicationId, req.shuffleId)
        val response = GetReducerFileGroupsResponse(
          status,
          stageResult.shuffleFileGroup,
          stageResult.mapperAttempts,
          stageResult.failedPartitionBatches
        )
        context.reply(response)
      }
    } else {
      context.reply(GetReducerFileGroupsResponse(status, null, null, null))
    }
  }

  def handleUnregisterShuffle(
      context: RpcCallContext,
      req: UnregisterShuffle): Unit = {
    val shuffleKey = Utils.getShuffleKey(req.applicationId, req.shuffleId)
    val validStageEnd =
      shuffleStageManager.finishShuffleStage(req.applicationId, req.shuffleId) == StageEndFinishStatus
    if (!validStageEnd) {
      // trigger StageEnd in case resource and file leak.
      val startMs = System.currentTimeMillis()
      handleStageEnd(null, StageEnd(req.applicationId, req.shuffleId))
      logInfo(s"handleStageEnd(null) for ${req.applicationId}-${req.shuffleId} " +
        s"take ${System.currentTimeMillis() - startMs} ms.")
    }

    shuffleStageManager.unregisterShuffle(req.applicationId, req.shuffleId)

    logInfo(s"UnregisterShuffle for shuffle $shuffleKey success.")
    if (context != null) {
      context.reply(UnregisterShuffleResponse(CssStatusCode.Success))
    }
  }

  def getShuffleAppManager(): ShuffleAppManager = {
    this.shuffleAppManager
  }

  def getShuffleStageManager(): ShuffleStageManager = {
    this.shuffleStageManager
  }

  def getShuffleWorkerManager(): ShuffleWorkerManager = {
    this.shuffleWorkerManager
  }
}

object Master extends Logging {

  // for test, local cluster
  @volatile
  var master: Master = null

  def getOrCreate(conf: CssConf): Master = getOrCreate(Utils.localHostName(), 0, conf)

  def getOrCreate(host: String, port: Int, conf: CssConf): Master = synchronized {
    if (master == null) {
      val rpcEnv = RpcEnv.create(RpcNameConstants.MASTER_SYS, host, port, conf)
      master = new Master(rpcEnv, conf)
      rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, master)
      ShutdownHookManager.get().addShutdownHook(new Thread {
        override def run(): Unit = {
          logInfo(s"Master shutting down, hook execution.")
          if (master != null) {
            rpcEnv.shutdown()
          }
        }
      }, 50)
      val heartbeatReceiver = new HeartbeatReceiver(rpcEnv, master)
      rpcEnv.setupEndpoint(RpcNameConstants.HEARTBEAT, heartbeatReceiver)
      master
    } else {
      master
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new CssConf()
    val masterArgs = new MasterArguments(args, conf)
    getOrCreate(masterArgs.host, masterArgs.port, conf).rpcEnv.awaitTermination()
  }
}
