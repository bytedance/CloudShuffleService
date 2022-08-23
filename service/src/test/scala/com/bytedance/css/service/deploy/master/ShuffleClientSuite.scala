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

import scala.collection.JavaConverters._

import com.bytedance.css.client.ShuffleClient
import com.bytedance.css.client.impl.ShuffleClientImpl
import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.CssRpcMessage.{BreakPartition, BreakPartitionResponse, GetReducerFileGroups, GetReducerFileGroupsResponse, RegisterPartitionGroup, RegisterPartitionGroupResponse}
import com.bytedance.css.common.protocol.CssStatusCode
import com.bytedance.css.common.util.Utils
import org.apache.commons.lang3.RandomStringUtils

class ShuffleClientSuite extends LocalClusterSuite {

  // here only use 2 workers to test.
  override def workersInitPorts: Seq[(Int, Int, Int)] = {
    Seq(
      (0, 0, 0),
      (0, 0, 0))
  }

  // set cssConf as common, because ShuffleClient is singleton
  val cssConf = new CssConf()
  cssConf.set("css.client.register.shuffle.retry.timeout", "12s")
  cssConf.set("css.client.register.shuffle.retry.init.interval", "500ms")
  cssConf.set("css.local.chunk.fetch.enabled", "false")
  cssConf.set("css.test.mode", "true")

  test("shuffle client singleton mode") {
    cssConf.set("css.master.address", masterRef.address.toCssURL)
    val shuffleClient1 = ShuffleClient.get(cssConf)
    val shuffleClient2 = ShuffleClient.get(cssConf)

    assert(shuffleClient1 == shuffleClient2);

    shuffleClient1.shutDown()
    shuffleClient2.shutDown()
  }

  Seq("DISK", "HDFS").foreach { mode =>
    test(s"shuffle client batch push data $mode") {
      cssConf.set("css.master.address", masterRef.address.toCssURL)
      cssConf.set("css.shuffle.mode", mode)

      val appId = s"appId-${System.currentTimeMillis()}"
      val shuffleId = 5
      val numMappers = 1
      val numPartitions = 1
      val maxPartitionsPerGroup = 1
      val mapperId = 0
      val mapperAttemptId = 0
      val reducerId = 0

      val shuffleClient = new ShuffleClientImpl(cssConf)
      ShuffleClient.cleanShuffle(shuffleId)

      val res1 = masterRef.askSync[RegisterPartitionGroupResponse](
        RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
      )
      assert(res1.statusCode == CssStatusCode.Success)
      assert(res1.partitionGroups.size() == 1)

      shuffleClient.applyShufflePartitionGroup(shuffleId, res1.partitionGroups)

      val resultSet = new java.util.HashSet[String]()
      var actualLength = 0L
      (0 until 100).foreach(t => {
        val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
        actualLength += content.length
        resultSet.add(content)
        val bytes = content.getBytes
        shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
          Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
      })
      // wait all batch push data request finish.
      Thread.sleep(5000)
      shuffleClient.mapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers)
      Thread.sleep(5000)

      val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
        GetReducerFileGroups(appId, shuffleId)
      )
      assert(reducerFileGroupsRes.status == CssStatusCode.Success)

      val inputStream = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

      var resultCount = 0L
      while (inputStream.read() != -1) {
        resultCount += 1
      }

      assert(actualLength == resultCount)
      inputStream.close()

      val inputStream2 = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

      val resultBytes = new Array[Byte](actualLength.toInt)
      var index = 0
      val buffer = new Array[Byte](1024)
      var readBytes = 0
      do {
        readBytes = inputStream2.read(buffer, 0, 1024)
        if (readBytes > 0) {
          System.arraycopy(buffer, 0, resultBytes, index, readBytes)
          index += readBytes
        }
      } while (readBytes != -1)

      var resultString = new String(resultBytes)
      resultSet.asScala.foreach(f => resultString.indexOf(f) != -1)
      resultSet.asScala.foreach{ f =>
        resultString = resultString.replace(f, "")
      }
      assert(resultString == "")

      inputStream2.close()
      shuffleClient.shutDown()
    }
  }

  test("shuffle client batch push data retry") {
    cssConf.set("css.master.address", masterRef.address.toCssURL)

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 10
    val numMappers = 1
    val numPartitions = 1
    val maxPartitionsPerGroup = 1
    val mapperId = 0
    val mapperAttemptId = 0
    val reducerId = 0

    val shuffleClient = ShuffleClient.get(cssConf)
    ShuffleClient.cleanShuffle(shuffleId)

    val res1 = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(res1.statusCode == CssStatusCode.Success)
    assert(res1.partitionGroups.size() == 1)

    shuffleClient.applyShufflePartitionGroup(shuffleId, res1.partitionGroups)
    val partitionGroup = res1.partitionGroups.asScala.head

    val resultSet = new java.util.HashSet[String]()
    var actualLength = 0L

    // just test retry to batch push data
    {
      val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
      actualLength += content.length
      resultSet.add(content)
      val bytes = content.getBytes
      shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
        Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
    }
    Thread.sleep(5000)

    // because here only 2 workers. so worker head must replica 0.
    // try set Exception for reduceId 0 epoch 0 & replica index 0.
    sendWorkerRpc[BreakPartition, BreakPartitionResponse](actualWorkers.head._1,
      BreakPartition(Utils.getShuffleKey(appId, shuffleId), reducerId, partitionGroup.epochId))
    Thread.sleep(2000)

    {
      val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
      actualLength += content.length
      resultSet.add(content)
      val bytes = content.getBytes
      shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
        Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
    }

    Thread.sleep(8000)
    shuffleClient.mapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers)

    Thread.sleep(2000)
    val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
      GetReducerFileGroups(appId, shuffleId)
    )

    var maxEpoch = -1
    reducerFileGroupsRes.fileGroup.foreach(reducerFileGroupsRes => {
      reducerFileGroupsRes.foreach(p => {
        if (p.getEpochId > maxEpoch) {
          maxEpoch = p.getEpochId
        }
      })
    })

    assert(reducerFileGroupsRes.status == CssStatusCode.Success)
    assert(maxEpoch == 1)

    val inputStream = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

    var resultCount = 0L
    while (inputStream.read() != -1) {
      resultCount += 1
    }

    assert(actualLength == resultCount)
    inputStream.close()

    val inputStream2 = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

    val resultBytes = new Array[Byte](actualLength.toInt)
    var index = 0
    val buffer = new Array[Byte](1024)
    var readBytes = 0
    do {
      readBytes = inputStream2.read(buffer, 0, 1024)
      if (readBytes > 0) {
        System.arraycopy(buffer, 0, resultBytes, index, readBytes)
        index += readBytes
      }
    } while (readBytes != -1)

    var resultString = new String(resultBytes)
    resultSet.asScala.foreach(f => resultString.indexOf(f) != -1)
    resultSet.asScala.foreach{ f =>
      resultString = resultString.replace(f, "")
    }
    assert(resultString == "")

    inputStream2.close()
    shuffleClient.shutDown()
  }

  test("skip batch push data after stage end") {
    cssConf.set("css.master.address", masterRef.address.toCssURL)

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 15
    val numMappers = 1
    val numPartitions = 1
    val maxPartitionsPerGroup = 1
    val mapperId = 0
    val mapperAttemptId = 0
    val reducerId = 0

    val shuffleClient = ShuffleClient.get(cssConf)
    ShuffleClient.cleanShuffle(shuffleId)

    val res1 = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
    )
    assert(res1.statusCode == CssStatusCode.Success)
    assert(res1.partitionGroups.size() == 1)

    shuffleClient.applyShufflePartitionGroup(shuffleId, res1.partitionGroups)

    val resultSet = new java.util.HashSet[String]()
    var actualLength = 0L
    (0 until 100).foreach(t => {
      val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
      actualLength += content.length
      resultSet.add(content)
      val bytes = content.getBytes
      shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
        Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
    })

    // trigger mapped end & stage end. so next batch push data will be skip.
    shuffleClient.mapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers)
    Thread.sleep(5000)

    // skip all batch push data.
    (0 until 5).foreach(t => {
      val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
      val bytes = content.getBytes
      shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
        Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
    })

    val inputStream = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

    var resultCount = 0L
    while (inputStream.read() != -1) {
      resultCount += 1
    }

    assert(actualLength == resultCount)
    inputStream.close()

    val inputStream2 = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), mapperId, mapperId + 1)

    val resultBytes = new Array[Byte](actualLength.toInt)
    var index = 0
    val buffer = new Array[Byte](1024)
    var readBytes = 0
    do {
      readBytes = inputStream2.read(buffer, 0, 1024)
      if (readBytes > 0) {
        System.arraycopy(buffer, 0, resultBytes, index, readBytes)
        index += readBytes
      }
    } while (readBytes != -1)

    var resultString = new String(resultBytes)
    resultSet.asScala.foreach(f => resultString.indexOf(f) != -1)
    resultSet.asScala.foreach{ f =>
      resultString = resultString.replace(f, "")
    }
    assert(resultString == "")

    inputStream2.close()
    shuffleClient.shutDown()
  }
}
