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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.bytedance.css.common.protocol.{CssStatusCode, PartitionGroup, PartitionInfo}
import com.bytedance.css.common.protocol.CssRpcMessage.{MapperEnd, MapperEndResponse, ReallocatePartitionGroup, ReallocatePartitionGroupResponse, RegisterPartitionGroup, RegisterPartitionGroupResponse}


class RegisterShuffleSuite extends LocalClusterSuite {

  test("register shuffle with same group") {
    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 5
    val numMappers = 10

    val numPartitions = 1002
    val maxPartitionsPerGroup = 20
    val res1 = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    val res2 = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(res1.statusCode == CssStatusCode.Success)
    assert(res2.statusCode == CssStatusCode.Success)
    assert(res1.partitionGroups.size() == res2.partitionGroups.size())
    res1.partitionGroups.asScala.indices.foreach { index =>
      assert(res1.partitionGroups.asScala(index) == res2.partitionGroups.asScala(index))
    }
  }

  test("register shuffle with diff group") {
    // register shuffle same like to assign strategy.
    // because there replica = 2 by default. so target possible tuple size = 3.

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 10
    val numMappers = 10

    // here use maxPartitionsPerGroup as group len.
    val numPartitions1 = 1002
    val maxPartitionsPerGroup1 = 20
    val res1 = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions1, maxPartitionsPerGroup1)
    )
    assert(res1.statusCode == CssStatusCode.Success)
    assert(res1.partitionGroups.size() == 51)
    res1.partitionGroups.asScala.foreach(p => assert(p.epochId == 0))
    assert(res1.partitionGroups.asScala(0).startPartition == 0)
    assert(res1.partitionGroups.asScala(0).endPartition == 20)
    assert(res1.partitionGroups.asScala(50).startPartition == 1000)
    assert(res1.partitionGroups.asScala(50).endPartition == 1002)

    // here use numPartitions / possible tuple size as group len.
    val numPartitions2 = 1002
    val maxPartitionsPerGroup2 = 1000
    val res2 = masterRef.askSync[RegisterPartitionGroupResponse](
      // register another shuffle's partition group
      RegisterPartitionGroup(appId, shuffleId + 1, numMappers, numPartitions2, maxPartitionsPerGroup2)
    )
    assert(res2.statusCode == CssStatusCode.Success)
    assert(res2.partitionGroups.size() == 3)
    res2.partitionGroups.asScala.foreach(p => assert(p.epochId == 0))
    assert(res2.partitionGroups.asScala(0).startPartition == 0)
    assert(res2.partitionGroups.asScala(0).endPartition == 334)
    assert(res2.partitionGroups.asScala(1).startPartition == 334)
    assert(res2.partitionGroups.asScala(1).endPartition == 668)
    assert(res2.partitionGroups.asScala(2).startPartition == 668)
    assert(res2.partitionGroups.asScala(2).endPartition == 1002)

    // here use one partition one group as group len.
    val numPartitions3 = 2
    val maxPartitionsPerGroup3 = 100
    val res3 = masterRef.askSync[RegisterPartitionGroupResponse](
      // register another shuffle's partition group
      RegisterPartitionGroup(appId, shuffleId + 2, numMappers, numPartitions3, maxPartitionsPerGroup3)
    )
    assert(res3.statusCode == CssStatusCode.Success)
    assert(res3.partitionGroups.size() == 2)
    res3.partitionGroups.asScala.foreach(p => assert(p.epochId == 0))
    assert(res3.partitionGroups.asScala(0).startPartition == 0)
    assert(res3.partitionGroups.asScala(0).endPartition == 1)
    assert(res3.partitionGroups.asScala(1).startPartition == 1)
    assert(res3.partitionGroups.asScala(1).endPartition == 2)
  }

  test("multi thread register shuffle like all mappers") {

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 15
    val numMappers = 10
    val numPartitions = 20
    val maxPartitionsPerGroup = 1

    val waitList: ArrayBuffer[RegisterPartitionGroupResponse] = new ArrayBuffer[RegisterPartitionGroupResponse]()
    val resultList: ArrayBuffer[RegisterPartitionGroupResponse] = new ArrayBuffer[RegisterPartitionGroupResponse]()

    // simulating 1000 mappers call register shuffle at the same time
    val threads = (0 until 1000).map(_ => {
      new Thread() {
        override def run(): Unit = {
          val res = masterRef.askSync[RegisterPartitionGroupResponse](
            RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
          )

          if (res.statusCode == CssStatusCode.Success) {
            resultList.synchronized {
              resultList.append(res)
            }
          } else if (res.statusCode == CssStatusCode.Waiting) {
            waitList.synchronized {
              waitList.append(res)
            }
          } else {
            assert(false)
          }
        }
      }
    })

    threads.foreach(_.start())
    Thread.sleep(3000)

    resultList.foreach(res => {
      assert(res.statusCode == CssStatusCode.Success)
      assert(res.partitionGroups.size() == 20)
      res.partitionGroups.asScala.foreach(p => {
        assert(p.epochId == 0)
        // skip Host check since all service in localhost
        p.getReplicaWorkers.asScala.indices.foreach { index =>
          val wPs = p.getReplicaWorkers.asScala.map(worker => worker.port).toSet
          assert(wPs.size == p.getReplicaWorkers.size())
        }
      })
    })
  }

  test("reallocate partition group") {

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 20
    val numMappers = 10
    val numPartitions = 20
    val maxPartitionsPerGroup = 1

    val res = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )

    assert(res.statusCode == CssStatusCode.Success)
    assert(res.partitionGroups.size() == 20)
    res.partitionGroups.asScala.foreach(p => assert(p.epochId == 0))

    val oldPartitionGroup = res.partitionGroups.asScala.head
    var reallocateRes = masterRef.askSync[ReallocatePartitionGroupResponse](
      ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup)
    )

    assert(reallocateRes.statusCode == CssStatusCode.Success)
    assert(reallocateRes.partitionGroup.partitionGroupId == oldPartitionGroup.partitionGroupId)
    assert(reallocateRes.partitionGroup.epochId == oldPartitionGroup.epochId + 1)
    assert(reallocateRes.partitionGroup.startPartition == oldPartitionGroup.startPartition)
    assert(reallocateRes.partitionGroup.endPartition == oldPartitionGroup.endPartition)

    // get from the newly one, no need to reallocate
    reallocateRes = masterRef.askSync[ReallocatePartitionGroupResponse](
      ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup)
    )
    assert(reallocateRes.statusCode == CssStatusCode.Success)
    assert(reallocateRes.partitionGroup.partitionGroupId == oldPartitionGroup.partitionGroupId)
    assert(reallocateRes.partitionGroup.epochId == oldPartitionGroup.epochId + 1)
    assert(reallocateRes.partitionGroup.startPartition == oldPartitionGroup.startPartition)
    assert(reallocateRes.partitionGroup.endPartition == oldPartitionGroup.endPartition)

    // reallocate a bigger one since last reallocate.
    val oldPartitionGroup2 = reallocateRes.partitionGroup
    reallocateRes = masterRef.askSync[ReallocatePartitionGroupResponse](
      ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup2)
    )
    assert(reallocateRes.statusCode == CssStatusCode.Success)
    assert(reallocateRes.partitionGroup.partitionGroupId == oldPartitionGroup.partitionGroupId)
    assert(reallocateRes.partitionGroup.epochId == oldPartitionGroup.epochId + 2)
    assert(reallocateRes.partitionGroup.startPartition == oldPartitionGroup.startPartition)
    assert(reallocateRes.partitionGroup.endPartition == oldPartitionGroup.endPartition)
  }

  test("reallocate partition group with unexpected status") {
    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 25
    val numMappers = 10
    val numPartitions = 20
    val maxPartitionsPerGroup = 1

    // since do not register shuffle.
    var oldPartitionGroup = new PartitionGroup(0, 0, 0, 0, null)
    var reallocateRes = masterRef.askSync[ReallocatePartitionGroupResponse](
      ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup)
    )
    assert(reallocateRes.statusCode == CssStatusCode.ShuffleNotRegistered)

    // start to register shuffle & mark current map task ended.
    val registerRes = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(registerRes.statusCode == CssStatusCode.Success)
    oldPartitionGroup = registerRes.partitionGroups.asScala.head

    val mapperRes = masterRef.askSync[MapperEndResponse](
      MapperEnd(appId, shuffleId, 0, 0, numMappers, new util.ArrayList[PartitionInfo], null)
    )
    assert(mapperRes.statusCode == CssStatusCode.Success)

    reallocateRes = masterRef.askSync[ReallocatePartitionGroupResponse](
      ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup)
    )
    assert(reallocateRes.statusCode == CssStatusCode.MapEnded)
  }

  test("multi thread reallocate partition group like all mappers") {
    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 30
    val numMappers = 10
    val numPartitions = 20
    val maxPartitionsPerGroup = 1

    // start to register shuffle
    val registerRes = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(registerRes.statusCode == CssStatusCode.Success)
    val oldPartitionGroup = registerRes.partitionGroups.asScala.head

    val waitList: ArrayBuffer[ReallocatePartitionGroupResponse] = new ArrayBuffer[ReallocatePartitionGroupResponse]()
    val resultList: ArrayBuffer[ReallocatePartitionGroupResponse] = new ArrayBuffer[ReallocatePartitionGroupResponse]()

    // simulating 1000 mappers call reallocate partition group at the same time
    val threads = (0 until 1000).map(_ => {
      new Thread() {
        override def run(): Unit = {
          val res = masterRef.askSync[ReallocatePartitionGroupResponse](
            ReallocatePartitionGroup(appId, shuffleId, 0, 0, oldPartitionGroup)
          )

          if (res.statusCode == CssStatusCode.Success) {
            resultList.synchronized {
              resultList.append(res)
            }
          } else if (res.statusCode == CssStatusCode.Waiting) {
            waitList.synchronized {
              waitList.append(res)
            }
          } else {
            assert(false)
          }
        }
      }
    })

    threads.foreach(_.start())
    Thread.sleep(3000)

    resultList.foreach(res => {
      assert(res.statusCode == CssStatusCode.Success)
      assert(res.partitionGroup.partitionGroupId == oldPartitionGroup.partitionGroupId)
      assert(res.partitionGroup.epochId == oldPartitionGroup.epochId + 1)
      assert(res.partitionGroup.startPartition == oldPartitionGroup.startPartition)
      assert(res.partitionGroup.endPartition == oldPartitionGroup.endPartition)
    })
  }
}
