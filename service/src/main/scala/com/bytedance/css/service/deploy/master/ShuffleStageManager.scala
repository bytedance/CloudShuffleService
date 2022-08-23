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

import com.bytedance.css.common.protocol.{CommittedPartitionInfo, FailedPartitionInfoBatch, PartitionGroup}

trait ShuffleStageManager {

  def registerShuffleStage(
      appId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      maxPartitionsPerGroup: Int): util.List[PartitionGroup]

  def validateRegisterShuffle(appId: String, shuffleId: Int): Boolean

  def getAppShuffle(appId: String): Set[String]

  def reallocateShufflePartition(
      appId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      oldPartitionGroup: PartitionGroup): PartitionGroup

  def commitShuffleStage(appId: String, shuffleId: Int, forceFinish: Boolean): Boolean

  def finishShuffleStage(appId: String, shuffleId: Int, wait: Boolean = false): StageStatus

  def getShuffleStageResult(appId: String, shuffleId: Int): StageResult

  def unregisterShuffle(appId: String, shuffleId: Int): Unit

  def start(): Unit

  def stop(): Unit
}


sealed trait StageStatus

case object NoStageEndStatus extends StageStatus

case object StageEndRunningStatus extends StageStatus

case object StageEndTimeOutStatus extends StageStatus

case object StageEndDataLostStatus extends StageStatus

case object StageEndFinishStatus extends StageStatus

case class StageResult(
    shuffleFileGroup: Array[Array[CommittedPartitionInfo]],
    mapperAttempts: Array[Int],
    failedPartitionBatches: util.HashSet[FailedPartitionInfoBatch])
