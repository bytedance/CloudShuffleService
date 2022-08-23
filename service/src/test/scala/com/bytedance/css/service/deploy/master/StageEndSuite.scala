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
import java.util.concurrent.CompletableFuture

import scala.collection.JavaConverters._

import com.bytedance.css.common.protocol.{CssStatusCode, PartitionInfo, ShuffleMode}
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.util.Utils
import org.apache.commons.lang3.RandomStringUtils

class StageEndSuite extends LocalClusterSuite {

  test("E2E Test for 3 * 5 mixed scenario") {
    // registerShuffle
    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 5
    val numMappers = 3
    val numPartitions = 5
    val maxPartitionsPerGroup = 1

    val res = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(res.statusCode == CssStatusCode.Success)
    assert(res.partitionGroups.size() == 5)

    val partitionGroups = res.partitionGroups

    // make partition index 0 empty
    // break partition index 1 for either replicas piece
    // other partition just normal push
    val allfutureList = new util.ArrayList[CompletableFuture[_]]()
    val futuresList = new util.ArrayList[Array[CompletableFuture[Int]]]()
    (0 until 10000).foreach(t => {
      val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
      val bytes = content.getBytes
      (1 until 5).foreach(index => {
        val pg = partitionGroups.get(index)
        val reducerId = pg.startPartition // because 1 partition 1 group. so sp is reduceId.
        val (allFuture, futures) = batchPushData(appId, shuffleId,
          Array(reducerId), pg.epochId, 0,
          bytes, Array(0, bytes.length), pg.getReplicaWorkers.asScala.toArray, ShuffleMode.DISK.toString)
        allfutureList.add(allFuture)
        futuresList.add(futures)
      })
    })
    allfutureList.asScala.foreach(_.get())
    assert(!futuresList.asScala.flatten.exists(_.get() != 0))

    // break index 1 partition's replica index 0 partition.
    val breakPartition = partitionGroups.get(1)
    val breakWorkerRpcPort = actualWorkers.filter(_._2 == breakPartition.getReplicaWorkers.get(0).port).map(_._1).head

    sendWorkerRpc[BreakPartition, BreakPartitionResponse](breakWorkerRpcPort,
      BreakPartition(Utils.getShuffleKey(appId, shuffleId),
        partitionGroups.get(1).startPartition, partitionGroups.get(1).epochId))

    // send mapper end
    val epochList = new util.ArrayList[PartitionInfo]()
    // initialPartitions index 0 has no data
    epochList.addAll((1 until 5).map(partitionGroups.get)
      .map(p => new PartitionInfo(p.startPartition, p.epochId)).asJava)
    masterRef.askSync[MapperEndResponse](MapperEnd(appId, shuffleId, mapId = 0, attemptId = 0, 3, epochList, null))
    masterRef.askSync[MapperEndResponse](MapperEnd(appId, shuffleId, mapId = 1, attemptId = 4, 3, epochList, null))
    masterRef.askSync[MapperEndResponse](MapperEnd(appId, shuffleId, mapId = 1, attemptId = 0, 3, epochList, null))
    masterRef.askSync[MapperEndResponse](MapperEnd(appId, shuffleId, mapId = 2, attemptId = 1, 3, epochList, null))

    // wait for trigger & finish stage end.
    Thread.sleep(5000)

    val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
      GetReducerFileGroups(appId, shuffleId)
    )

    assert(reducerFileGroupsRes.status == CssStatusCode.Success)
    // filter two empty piece and a broken one
    assert(reducerFileGroupsRes.fileGroup.flatten.length == 7)
    // for partition 1 mapper attempt 4 comes first.
    reducerFileGroupsRes.attempts.zip(Array(0, 4, 1)).foreach(f => assert(f._1 == f._2))
  }
}
