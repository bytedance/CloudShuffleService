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
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.{CommittedPartitionInfo, PartitionGroup}
import com.bytedance.css.common.protocol.CssRpcMessage.{CommitFiles, CommitFilesResponse}
import com.bytedance.css.common.rpc.RpcTimeout
import com.bytedance.css.common.util.{ThreadUtils, Utils}
import com.bytedance.css.common.util.Collections._
import com.bytedance.css.service.deploy.common.ScheduledManager
import com.bytedance.css.service.deploy.metadata.ExternalShuffleMeta
import io.netty.util.internal.ConcurrentSet

class ShuffleStageManagerImpl(
    cssConf: CssConf,
    assignStrategy: AssignStrategy,
    shuffleTaskManager: ShuffleTaskManager,
    shuffleWorkerManager: ShuffleWorkerManager,
    externalShuffleMeta: ExternalShuffleMeta) extends ShuffleStageManager with Logging {

  // store all registered shuffle and its partition group.
  // key appId-shuffleId
  private[deploy] val registeredPartitionGroup = new ConcurrentHashMap[String, util.List[PartitionGroup]]()

  // store all reallocated shuffle and its current max epoch.
  // key appId-shuffleId   partitionGroupId
  private[deploy] val reallocatedPGMaxEpochMap =
    new ConcurrentHashMap[String, ConcurrentHashMap[Int, PartitionGroup]]()

  private[deploy] val stageEndShuffleSet = new ConcurrentSet[String]()
  private[deploy] val stageEndShuffleTimeMap = new ConcurrentHashMap[String, Long]()

  private[deploy] val dataLostShuffleSet = new ConcurrentSet[String]()

  private[deploy] val unregisterShuffleTime = new ConcurrentHashMap[String, Long]()
  private val removeShuffleDelayMs = CssConf.removeShuffleDelayMs(cssConf)
  private[deploy] val scheduledManager = new ScheduledManager("shuffle-forward-message-thread", 1)
  scheduledManager.addScheduledTask("shuffleTimeOutTask", checkExpiredShuffle, 0, removeShuffleDelayMs)

  override def registerShuffleStage(
      appId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      maxPartitionsPerGroup: Int): util.List[PartitionGroup] = {
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)

    if (registeredPartitionGroup.get(shuffleKey) == null) {
      shuffleKey.intern().synchronized {
        if (registeredPartitionGroup.get(shuffleKey) == null) {
          val startMs = System.currentTimeMillis()

          // try to assign partition group with current workers.
          val stableAssignableWorkers = shuffleWorkerManager.getStableAssignableWorkers()
          val partitionGroupList = assignStrategy.assignPartitionGroup(
            numPartitions,
            maxPartitionsPerGroup,
            stableAssignableWorkers
          )

          externalShuffleMeta.shuffleCreated(shuffleKey)
          shuffleTaskManager.initShuffleTask(shuffleKey, numMappers, numPartitions)

          val replicaWorks = partitionGroupList.flatMap(partitionGroup => partitionGroup.replicaWorkers.asScala)
          shuffleWorkerManager.addShuffleStageWorker(shuffleKey, replicaWorks)

          // register shuffle done
          logInfo(s"RegisterPartitionGroup for $shuffleKey ${numMappers} X ${numPartitions} " +
            s"maxPartitionsPerGroup: ${maxPartitionsPerGroup} " +
            s"used ${System.currentTimeMillis() - startMs}ms")
          registeredPartitionGroup.put(shuffleKey, partitionGroupList.asJava)
        }
      }
    }
    registeredPartitionGroup.get(shuffleKey)
  }

  override def validateRegisterShuffle(appId: String, shuffleId: Int): Boolean = {
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    registeredPartitionGroup.containsKey(shuffleKey)
  }

  override def getAppShuffle(appId: String): Set[String] = {
    registeredPartitionGroup.keys().asScala.filter(_.startsWith(appId)).toSet
  }

  override def reallocateShufflePartition(
      appId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      oldPartitionGroup: PartitionGroup): PartitionGroup = {
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    val partitionGroupId = oldPartitionGroup.partitionGroupId

    def biggerEpochExists(): Boolean = {
      val opPg = Option(reallocatedPGMaxEpochMap.get(shuffleKey)).map(_.get(partitionGroupId))
      if (opPg.isEmpty || opPg.get == null) {
        return false
      }
      opPg.get.epochId > oldPartitionGroup.epochId
    }

    def createBiggerEpoch(): PartitionGroup = {
      val assignableWorkers = shuffleWorkerManager.getStableAssignableWorkers()
      val partitionGroup = assignStrategy.reallocatePartitionGroup(assignableWorkers, oldPartitionGroup)

      if (partitionGroup == null) {
        logError(s"assignGroupsWithRetry failed to reallocate after ${assignableWorkers.length} attempts.")
        return null
      }
      val maxEpochMap = reallocatedPGMaxEpochMap
        .computeWhenAbsent(shuffleKey, _ => { new ConcurrentHashMap[Int, PartitionGroup] })
      maxEpochMap.put(partitionGroup.partitionGroupId, partitionGroup)
      return partitionGroup
    }

    if (!biggerEpochExists()) {
      reallocatedPGMaxEpochMap.synchronized {
        if (!biggerEpochExists()) {
          val newPartitionGroup = createBiggerEpoch()
          if (newPartitionGroup == null) {
            return null
          }
          val reallocateReplicaWorkers =
            reallocatedPGMaxEpochMap.get(shuffleKey).get(partitionGroupId).replicaWorkers.asScala.toList
          shuffleWorkerManager.addShuffleStageWorker(shuffleKey, reallocateReplicaWorkers)
        }
      }
    }
    reallocatedPGMaxEpochMap.get(shuffleKey).get(partitionGroupId)
  }

  override def commitShuffleStage(appId: String, shuffleId: Int, forceFinish: Boolean): Boolean = {

    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)

    stageEndShuffleTimeMap.putIfAbsent(shuffleKey, System.currentTimeMillis())
    if (forceFinish) {
      stageEndShuffleSet.add(shuffleKey)
      return true
    }

    // whether the worker is alive or not, we need to CommitFiles.
    val shuffleWorker = shuffleWorkerManager.getShuffleStageWorker(shuffleKey)

    val commitPieces = new ConcurrentHashMap[String, ConcurrentSet[CommittedPartitionInfo]]
    val parallelism = Math.min(shuffleWorker.size, CssConf.commitFilesParallelism(cssConf))
    ThreadUtils.parmap(shuffleWorker.to,
      "SendCommitFilesRequest", parallelism) { worker =>
      try {
        val committed = worker.workerRpcRef.askSync[CommitFilesResponse](
          CommitFiles(shuffleKey),
          new RpcTimeout(new FiniteDuration(CssConf.stageEndTimeoutMs(cssConf), MILLISECONDS),
            "css.stage.end.timeout")).committed
        // getFilePath == null means flush failed, piece is invalid.
        committed.asScala.filter(_ != null).filter(_.getFilePath != null)
          .foreach(p => {
            val epochKey = p.getEpochKey
            if (p.getFilePath.endsWith("data")) {
              val commits = commitPieces.computeWhenAbsent(epochKey, _ => {new ConcurrentSet[CommittedPartitionInfo]})
              commits.add(p)
            } else {
              throw new Exception("filePath should either endsWith data.")
            }
          })
      } catch {
        case ex: Exception =>
          // Log and do nothing for now.
          logError(s"AskSync CommitFiles to Worker ${worker} for ${shuffleKey} failed.", ex)
      }
    }

    // after CommitFiles finished, clean all shuffle workers.
    shuffleWorkerManager.removeShuffleStageWorker(shuffleKey)

    // commit all task partition info & check data lost
    val dataLost = shuffleTaskManager.commitAllTaskPartitionInfo(shuffleKey, commitPieces)
    if (dataLost) {
      dataLostShuffleSet.add(shuffleKey)
    }
    stageEndShuffleSet.add(shuffleKey)
    dataLost
  }

  override def finishShuffleStage(appId: String, shuffleId: Int, wait: Boolean = false): StageStatus = {
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    val stageEndTimeoutMs = CssConf.stageEndTimeoutMs(cssConf)
    val waitIntervalMs = CssConf.stageEndRetryIntervalMs(cssConf)
    var loop = true
    var status: StageStatus = null
    while (loop) {
      if (!stageEndShuffleTimeMap.containsKey(shuffleKey)) {
        // no stage end request handled, should just return no stage end.
        status = NoStageEndStatus
        loop = false
      } else if (!stageEndShuffleSet.contains(shuffleKey)) {
        val stageEndStartMs = stageEndShuffleTimeMap.get(shuffleKey)
        if (System.currentTimeMillis() - stageEndStartMs > stageEndTimeoutMs) {
          // stageEnd timeout
          status = StageEndTimeOutStatus
          loop = false
        } else {
          // stageEnd processing
          if (wait) {
            TimeUnit.MILLISECONDS.sleep(waitIntervalMs)
          } else {
            status = StageEndRunningStatus
            loop = false
          }
        }
      } else if (dataLostShuffleSet.contains(shuffleKey)) {
        status = StageEndDataLostStatus
        loop = false
      } else {
        status = StageEndFinishStatus
        loop = false
      }
    }
    status
  }

  override def getShuffleStageResult(appId: String, shuffleId: Int): StageResult = {
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    val (shuffleFileGroup, mapperAttempts, failedPartitionBatches) =
      shuffleTaskManager.getReducerTaskFileGroups(shuffleKey)
    StageResult(shuffleFileGroup, mapperAttempts, failedPartitionBatches)
  }

  override def unregisterShuffle(appId: String, shuffleId: Int): Unit = {
    // record the unregister shuffle request time, and delay unregister later via timer.
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    unregisterShuffleTime.put(shuffleKey, System.currentTimeMillis())
  }

  private def checkExpiredShuffle(): Unit = {
    val currentTime = System.currentTimeMillis()
    logDebug(s"Check for expired shuffle with $currentTime")
    val keys = unregisterShuffleTime.keys().asScala.toList
    val expiredKeys = keys.map { shuffleKey =>
      if (unregisterShuffleTime.get(shuffleKey) < currentTime - removeShuffleDelayMs) {
        logInfo(s"Actually UnregisterShuffle handle with $currentTime for shuffle $shuffleKey")
        registeredPartitionGroup.remove(shuffleKey)
        reallocatedPGMaxEpochMap.remove(shuffleKey)
        stageEndShuffleSet.remove(shuffleKey)
        stageEndShuffleTimeMap.remove(shuffleKey)
        dataLostShuffleSet.remove(shuffleKey)
        unregisterShuffleTime.remove(shuffleKey)
        shuffleWorkerManager.removeShuffleStageWorker(shuffleKey)
        shuffleTaskManager.removeAllShuffleTask(shuffleKey)
        shuffleKey
      } else null
    }.filter(_ != null)
    externalShuffleMeta.shuffleRemoved(expiredKeys.toSet)
  }

  override def start(): Unit = {
    scheduledManager.start()
  }

  override def stop(): Unit = {
    scheduledManager.stop()
  }

}
