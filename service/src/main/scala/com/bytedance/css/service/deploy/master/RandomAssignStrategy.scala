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

import java.util.Random

import scala.collection.JavaConverters._

import com.bytedance.css.common.protocol.{PartitionGroup, WorkerAddress}
import com.bytedance.css.service.deploy.worker.WorkerInfo

class RandomAssignStrategy extends AssignStrategy {

  lazy val rand = new Random

  override def assignPartitionGroup(
      numPartitions: Int,
      maxPartitionsPerGroup: Int,
      assignableWorkers: Seq[WorkerInfo]): List[PartitionGroup] = {
    val groupInfo = calculateGroupInfo(numPartitions, maxPartitionsPerGroup, assignableWorkers)
    val groupNum = groupInfo.groupNum
    val groupLength = groupInfo.groupLength
    val possibleTuples = groupInfo.possibleTuples

    val groupPairs = (0 until groupNum).map(_ => {
      val pairIndex = possibleTuples(rand.nextInt(possibleTuples.size))
      val assignWorkers = pairIndex.map(i => assignableWorkers(i))
        .filter(workerInfo => workerInfo.isActive())
      assignWorkers.map(worker => new WorkerAddress(worker.host, worker.pushPort))
    })

    val partitionGroupList = groupPairs.zipWithIndex.map(f => {
      // Register EpochId should all be 0
      new PartitionGroup(f._2,
        0,
        f._2 * groupLength,
        Math.min((f._2 + 1) * groupLength, numPartitions),
        f._1.asJava
      )
    })
    partitionGroupList.toList
  }

  override def reallocatePartitionGroup(
      assignableWorkers: Seq[WorkerInfo],
      oldPartitionGroup: PartitionGroup): PartitionGroup = {
    val possibleTuples = calculatePossibleTuples(assignableWorkers)
    val tmpGroupPair = {
      val pairIndex = possibleTuples(rand.nextInt(possibleTuples.size))
      val assignWorkers = pairIndex.map(i => assignableWorkers(i))
        .filter(workerInfo => workerInfo.isActive())
      assignWorkers.map(worker => new WorkerAddress(worker.host, worker.pushPort))
    }
    val partitionGroup = new PartitionGroup(
      oldPartitionGroup.partitionGroupId,
      oldPartitionGroup.epochId + 1,
      oldPartitionGroup.startPartition,
      oldPartitionGroup.endPartition,
      tmpGroupPair.asJava)
    partitionGroup
  }
}
