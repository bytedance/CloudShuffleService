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

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.{CommittedPartitionInfo, FailedPartitionInfoBatch, PartitionInfo}
import com.bytedance.css.common.util.Collections._
import io.netty.util.internal.ConcurrentSet

class ShuffleTaskManager extends Logging {

  // key appId-shuffleId
  private[deploy] val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  private[deploy] val reducerFileGroupsMap =
    new ConcurrentHashMap[String, Array[Array[CommittedPartitionInfo]]]()

  // key appId-shuffleId
  // store all reducerId-EpochId for a shuffleKey
  private[deploy] val shuffleEpochSetMap =
    new ConcurrentHashMap[String, util.HashSet[PartitionInfo]]()
  private[deploy] val batchBlacklistMap =
    new ConcurrentHashMap[String, ConcurrentHashMap[Int, util.List[FailedPartitionInfoBatch]]]()

  def initShuffleTask(
      shuffleKey: String,
      numMappers: Int,
      numPartitions: Int): Unit = {
    shuffleMapperAttempts.putIfAbsent(shuffleKey, Array.fill(numMappers)(-1))
    shuffleEpochSetMap.putIfAbsent(shuffleKey, new util.HashSet[PartitionInfo]()))
    reducerFileGroupsMap.putIfAbsent(shuffleKey, new Array[Array[CommittedPartitionInfo]](numPartitions))
  }

  def shuffleTaskEnded(
      shuffleKey: String,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      epochList: util.List[PartitionInfo],
      batchBlacklist: util.List[FailedPartitionInfoBatch]): Unit = {
    shuffleMapperAttempts.synchronized {
      var attempts = shuffleMapperAttempts.get(shuffleKey)
      // for empty shuffle, e2e process could be non-register-shuffle at all
      // and all mapper task start sending out MapperEnd after empty iterator
      if (attempts == null) {
        logWarning(s"Null shuffleMapperAttempts for shuffle $shuffleKey, create shuffleMapperAttempts.")
        attempts = Array.fill(numMappers)(-1)
        shuffleMapperAttempts.put(shuffleKey, attempts)
      }

      // epochList not be null.
      val epochSet = shuffleEpochSetMap.computeWhenAbsent(shuffleKey, _ => {
        new util.HashSet[PartitionInfo]()
      })
      epochSet.addAll(epochList)

      if (batchBlacklist != null) {
        val blacklist = batchBlacklistMap.computeWhenAbsent(shuffleKey, _ => {
          new ConcurrentHashMap[Int, util.List[FailedPartitionInfoBatch]]
        })
        blacklist.put(mapId, batchBlacklist)
      }

      // only remain the first success mapper & skip another attempt called.
      if (attempts(mapId) < 0) {
        attempts(mapId) = attemptId
      }
    }
  }

  def validateShuffleTaskEnded(
      shuffleKey: String,
      mapId: Int): Boolean = {
    // MapperAttempt != -1 means already has mapper calling MapperEnd
    shuffleMapperAttempts.containsKey(shuffleKey) && shuffleMapperAttempts.get(shuffleKey)(mapId) != -1
  }

  def validateEffectiveTaskAttempt(
      shuffleKey: String,
      mapId: Int,
      attemptId: Int): Boolean = {
    shuffleMapperAttempts.containsKey(shuffleKey) && shuffleMapperAttempts.get(shuffleKey)(mapId) == attemptId
  }

  def validateAllTaskEnded(shuffleKey: String): Boolean = {
    shuffleMapperAttempts.containsKey(shuffleKey) && !shuffleMapperAttempts.get(shuffleKey).exists(_ < 0)
  }

  def commitAllTaskPartitionInfo(
      shuffleKey: String,
      commitPieces: ConcurrentHashMap[String, ConcurrentSet[CommittedPartitionInfo]]): Boolean = {

    // check for data lost
    val allEpochSets = shuffleEpochSetMap.getOrDefault(shuffleKey, new util.HashSet[PartitionInfo]())

    var dataLost = false
    val validCommitted = allEpochSets.asScala
      .flatMap(partitionInfo => {
        val epochKey = partitionInfo.getEpochKey
        val commits = commitPieces.getOrDefault(epochKey, null)
        if (commits == null) {
          dataLost = true
          logError(s"dataLost for $shuffleKey $epochKey, all replica null")
        }
        val availCommits = commits.asScala.filter(_ != null).filter(_.getFileLength >= 0).toSeq
        if (availCommits.isEmpty) {
          dataLost = true
          logError(s"dataLost for $shuffleKey $epochKey, all replica file len < 0")
        }
        availCommits
      })
      .filter(_ != null)
      .filter(_.getFileLength > 0)

    if (!dataLost) {
      val fileGroups = reducerFileGroupsMap.get(shuffleKey)
      val sets = Array.fill(fileGroups.length)(new util.HashSet[CommittedPartitionInfo]())
      validCommitted.foreach { partition =>
        sets(partition.getReducerId).add(partition)
      }
      (0 until fileGroups.length).foreach(i => {
        fileGroups(i) = sets(i).asScala.toSeq.toArray
      })

      var shuffleSize: Long = 0
      (0 until fileGroups.length).foreach(i => {
        if (fileGroups(i) != null && fileGroups(i).length > 0) {
          shuffleSize = shuffleSize + fileGroups(i).last.getFileLength
        }
      })
      logInfo(s"total shuffle size for shuffleKey ${shuffleKey} size ${shuffleSize}")
    }

    dataLost
  }

  def getReducerTaskFileGroups(shuffleKey: String):
      (Array[Array[CommittedPartitionInfo]], Array[Int], util.HashSet[FailedPartitionInfoBatch]) = {
    val shuffleFileGroup = reducerFileGroupsMap.get(shuffleKey)
    val mapperAttempts = shuffleMapperAttempts.get(shuffleKey)
    val failedPartitionBatches = if (batchBlacklistMap.get(shuffleKey) != null) {
      val allBatches = new util.HashSet[FailedPartitionInfoBatch]()
      allBatches.addAll(
        batchBlacklistMap.getOrDefault(shuffleKey, new ConcurrentHashMap[Int, util.List[FailedPartitionInfoBatch]]())
          .values().asScala.flatMap(x => x.asScala.toSet[FailedPartitionInfoBatch]).toSet.asJava)
      allBatches
    } else {
      null
    }
    (shuffleFileGroup, mapperAttempts, failedPartitionBatches)
  }

  def removeAllShuffleTask(shuffleKey: String): Unit = {
    shuffleMapperAttempts.remove(shuffleKey)
    shuffleEpochSetMap.remove(shuffleKey)
    reducerFileGroupsMap.remove(shuffleKey)
    batchBlacklistMap.remove(shuffleKey)
  }
}
