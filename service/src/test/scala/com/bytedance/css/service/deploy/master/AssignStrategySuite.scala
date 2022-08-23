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
import com.bytedance.css.common.protocol.PartitionGroup
import com.bytedance.css.common.rpc.RpcEndpointRef
import com.bytedance.css.service.deploy.worker.WorkerInfo
import org.scalatest.FunSuite

class AssignStrategySuite extends FunSuite {

  val cssConf = new CssConf()
  val assignStrategy: AssignStrategy = AssignStrategy.buildAssignStrategy(cssConf)

  val assignableWorkers = Seq(
    new MockWorkerInfo("NeverMind"),
    new MockWorkerInfo("NeverMind"),
    new MockWorkerInfo("NeverMind"),
    new MockWorkerInfo("NeverMind"),
    new MockWorkerInfo("NeverMind"),
    new MockWorkerInfo("NeverMind")
  )

  class MockWorkerInfo(
      host: String,
      rpcPort: Int = 1,
      pushPort: Int = 1,
      fetchPort: Int = 1,
      workerRpcRef: RpcEndpointRef = null) extends WorkerInfo(host, rpcPort, pushPort, fetchPort, workerRpcRef) {
    override def isActive(): Boolean = true
  }

  test("assign partition group with diff group len") {
    // because there replica = 2 by default. so target possible tuple size = 15.

    // here use maxPartitionsPerGroup as group len.
    val numPartitions1 = 1002
    val maxPartitionsPerGroup1 = 20
    var partitionGroups =
      assignStrategy.assignPartitionGroup(numPartitions1, maxPartitionsPerGroup1, assignableWorkers)

    assert(partitionGroups.size == 51)
    partitionGroups.foreach(p => assert(p.epochId == 0))

    assert(partitionGroups(0).startPartition == 0)
    assert(partitionGroups(0).endPartition == 20)
    assert(partitionGroups(0).replicaWorkers.size() == 2)
    assert(partitionGroups(50).startPartition == 1000)
    assert(partitionGroups(50).endPartition == 1002)
    assert(partitionGroups(50).replicaWorkers.size() == 2)

    // here use numPartitions / possible tuple size as group len.
    val numPartitions2 = 1002
    val maxPartitionsPerGroup2 = 1000
    partitionGroups =
      assignStrategy.assignPartitionGroup(numPartitions2, maxPartitionsPerGroup2, assignableWorkers)

    assert(partitionGroups.size == 16)
    partitionGroups.foreach(p => assert(p.epochId == 0))

    assert(partitionGroups(0).startPartition == 0)
    assert(partitionGroups(0).endPartition == 66)
    assert(partitionGroups(0).replicaWorkers.size() == 2)
    assert(partitionGroups(15).startPartition == 990)
    assert(partitionGroups(15).endPartition == 1002)
    assert(partitionGroups(15).replicaWorkers.size() == 2)

    // here use numPartitions / worker size as group len.
    val numPartitions3 = 12
    val maxPartitionsPerGroup3 = 1000
    partitionGroups =
      assignStrategy.assignPartitionGroup(numPartitions3, maxPartitionsPerGroup3, assignableWorkers)

    assert(partitionGroups.size == 6)
    partitionGroups.foreach(p => assert(p.epochId == 0))

    assert(partitionGroups(0).startPartition == 0)
    assert(partitionGroups(0).endPartition == 2)
    assert(partitionGroups(0).replicaWorkers.size() == 2)
    assert(partitionGroups(5).startPartition == 10)
    assert(partitionGroups(5).endPartition == 12)
    assert(partitionGroups(5).replicaWorkers.size() == 2)

    // here use one partition one group as group len.
    val numPartitions4 = 5
    val maxPartitionsPerGroup4 = 100
    partitionGroups =
      assignStrategy.assignPartitionGroup(numPartitions4, maxPartitionsPerGroup4, assignableWorkers)

    assert(partitionGroups.size == 5)
    partitionGroups.foreach(p => assert(p.epochId == 0))

    assert(partitionGroups(0).startPartition == 0)
    assert(partitionGroups(0).endPartition == 1)
    assert(partitionGroups(0).replicaWorkers.size() == 2)
    assert(partitionGroups(4).startPartition == 4)
    assert(partitionGroups(4).endPartition == 5)
    assert(partitionGroups(4).replicaWorkers.size() == 2)
  }

  test("reallocate partition group") {
    val oldPartitionGroup = new PartitionGroup(0, 0, 0, 10, null)

    val newPartitionGroup1 = assignStrategy.reallocatePartitionGroup(assignableWorkers, oldPartitionGroup)

    assert(newPartitionGroup1.partitionGroupId == oldPartitionGroup.partitionGroupId)
    assert(newPartitionGroup1.epochId == oldPartitionGroup.epochId + 1)
    assert(newPartitionGroup1.startPartition == oldPartitionGroup.startPartition)
    assert(newPartitionGroup1.endPartition == oldPartitionGroup.endPartition)
    assert(newPartitionGroup1.replicaWorkers.size() == 2)

    val newPartitionGroup2 = assignStrategy.reallocatePartitionGroup(assignableWorkers, newPartitionGroup1)

    assert(newPartitionGroup2.partitionGroupId == newPartitionGroup1.partitionGroupId)
    assert(newPartitionGroup2.epochId == newPartitionGroup1.epochId + 1)
    assert(newPartitionGroup2.startPartition == newPartitionGroup1.startPartition)
    assert(newPartitionGroup2.endPartition == newPartitionGroup1.endPartition)
    assert(newPartitionGroup2.replicaWorkers.size() == 2)
  }

}
