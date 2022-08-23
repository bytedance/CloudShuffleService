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

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.PartitionGroup
import com.bytedance.css.service.deploy.worker.WorkerInfo

trait AssignStrategy extends Logging {

  /**
   * if target numPartition less than worker combination numbers,
   * Basically, it can indeed be a relatively small shuffle,
   * but we still don’t want each partition to have a group,
   * so the efficiency of push data will be relatively poor,
   * so in this case, the step size can be defined as numPartition / workerSize.
   * If numPartition is smaller than workerSize, then there is no way,
   * it can only be a group according to 1 partition.
   */
  protected def calculateGroupInfo(
      numPartitions: Int,
      maxPartitionsPerGroup: Int,
      assignableWorkers: Seq[WorkerInfo],
      replica: Int = 2): GroupInfo = {

    val possibleTuples = calculatePossibleTuples(assignableWorkers, replica)
    val tupleSize = possibleTuples.size

    val groupLength = if (numPartitions < tupleSize) {
      Math.max(1, numPartitions / assignableWorkers.size)
    } else {
      Math.min(numPartitions / tupleSize, maxPartitionsPerGroup)
    }

    val groupNum = if (numPartitions % groupLength == 0) {
      numPartitions / groupLength
    } else {
      (numPartitions + groupLength) / groupLength
    }
    GroupInfo(groupLength, groupNum, possibleTuples)
  }

  protected def calculatePossibleTuples(
      assignableWorkers: Seq[WorkerInfo],
      replica: Int = 2): List[List[Int]] = {
    if (replica > assignableWorkers.size) {
      throw new RuntimeException("replica num must less than worker num")
    }
    val indices = assignableWorkers.indices.toList
    indices.combinations(replica).toList
  }

  /**
   * Assign partition group to current shuffle.
   */
  def assignPartitionGroup(
      numPartitions: Int,
      maxPartitionsPerGroup: Int,
      assignableWorkers: Seq[WorkerInfo]): List[PartitionGroup]

  /**
   * Reallocate a new partition group with epochId which big than old partition group.
   */
  def reallocatePartitionGroup(
      assignableWorkers: Seq[WorkerInfo],
      oldPartitionGroup: PartitionGroup): PartitionGroup
}

case class GroupInfo(groupLength: Int, groupNum: Int, possibleTuples: List[List[Int]])

object AssignStrategy {

  def buildAssignStrategy(conf: CssConf): AssignStrategy = {
    val assignStrategy = CssConf.partitionAssignStrategy(conf)
    // scalastyle:off caselocale
    assignStrategy.toUpperCase match {
      case "RANDOM" => new RandomAssignStrategy
      case _ => throw new RuntimeException("not support assign strategy: " + assignStrategy)
    }
    // scalastyle:on caselocale
  }
}
