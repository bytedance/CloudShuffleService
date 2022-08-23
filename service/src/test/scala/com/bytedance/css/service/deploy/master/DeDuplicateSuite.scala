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
import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.protocol.CssStatusCode
import com.bytedance.css.common.util.Utils
import org.apache.commons.lang3.RandomStringUtils

class DeDuplicateSuite extends LocalClusterSuite {

  Seq(true, false).foreach { mode =>
    test(s"failedBatchBlacklistEnable $mode") {
      val cssConf: CssConf = new CssConf()
      cssConf.set("css.master.address", masterRef.address.toCssURL)
      cssConf.set("css.client.failed.batch.blacklist.enabled", mode.toString)
      cssConf.set("css.local.chunk.fetch.enabled", "false")
      cssConf.set("css.test.mode", "true")

      val appId = s"appId-${System.currentTimeMillis()}"
      val shuffleId = 5
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
      val workers = partitionGroup.getReplicaWorkers.asScala.toArray

      val resultSet = new java.util.HashMap[String, Int]()
      (0 until 100).foreach(t => {
        // send pushData to replica piece.
        val content = RandomStringUtils.randomAlphanumeric(10, 20)
        if (resultSet.containsKey(content)) {
          resultSet.put(content, resultSet.get(content) + 1)
        } else {
          resultSet.put(content, 1)
        }
        val bytes = (content + ",").getBytes
        shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
          Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
      })
      // wait for all batch push data write to partition file.
      Thread.sleep(2000)

      val replica1PartitionPort = actualWorkers.filter(_._2 == workers(0).port).head._1

      sendWorkerRpc[BreakPartition, BreakPartitionResponse](replica1PartitionPort,
        BreakPartition(Utils.getShuffleKey(appId, shuffleId), reducerId, partitionGroup.epochId))
      Thread.sleep(2000)

      (0 until 100).foreach(t => {
        val content = RandomStringUtils.randomAlphanumeric(10, 20)
        if (resultSet.containsKey(content)) {
          resultSet.put(content, resultSet.get(content) + 1)
        } else {
          resultSet.put(content, 1)
        }
        val bytes = (content + ",").getBytes
        shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
          Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
      })

      // trigger mapped end & stage end.
      shuffleClient.mapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers)
      Thread.sleep(5000)

      val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
        GetReducerFileGroups(appId, shuffleId))

      if (mode) {
        assert(reducerFileGroupsRes.batchBlacklist != null)
        assert(reducerFileGroupsRes.batchBlacklist.size() >= 1)
        assert(reducerFileGroupsRes.batchBlacklist.asScala.forall(_.getFailedPartitionBatchStr.startsWith("0-0-0-0-")))
      } else {
        assert(reducerFileGroupsRes.batchBlacklist == null)
      }

      // read two epochs separately
      val inputStream1 = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), 2, 0)
      val inputStream2 = shuffleClient.readPartitions(appId, shuffleId, Array(reducerId), 2, 1)

      val resultBytes = new Array[Byte](200 * 21 + 2000)
      var index = 0
      val buffer = new Array[Byte](1024)
      var readBytes = 0
      do {
        readBytes = inputStream1.read(buffer, 0, 1024)
        if (readBytes > 0) {
          System.arraycopy(buffer, 0, resultBytes, index, readBytes)
          index += readBytes
        }
      } while (readBytes != -1)

      readBytes = 0
      do {
        readBytes = inputStream2.read(buffer, 0, 1024)
        if (readBytes > 0) {
          System.arraycopy(buffer, 0, resultBytes, index, readBytes)
          index += readBytes
        }
      } while (readBytes != -1)

      val resultStr = new String(resultBytes, 0, index)
      val actualSet = new java.util.HashMap[String, Int]()
      resultStr.split(",").filter(_.nonEmpty)
        .foreach(content => {
          if (actualSet.containsKey(content)) {
            actualSet.put(content, actualSet.get(content) + 1)
          } else {
            actualSet.put(content, 1)
          }
        })

      inputStream1.close()
      inputStream2.close()

      assert(!resultSet.values().asScala.exists(_ > 1))
      assert(resultSet.size() == actualSet.size())
      if (mode) {
        assert(actualSet.values().asScala.count(_ > 1) == 0)
      } else {
        assert(actualSet.values().asScala.count(_ > 1) >= 1)
      }

      // the two epochs are read as a whole, there should be no duplication.
      val asSinglePartitionBytes = new Array[Byte](200 * 21 + 2000)
      val inputStream = shuffleClient.readPartitions(appId, shuffleId, Array(0), 0, 1)
      readBytes = 0
      index = 0
      do {
        readBytes = inputStream.read(buffer, 0, 1024)
        if (readBytes > 0) {
          System.arraycopy(buffer, 0, asSinglePartitionBytes, index, readBytes)
          index += readBytes
        }
      } while (readBytes != -1)
      val singlePartitionResultStr = new String(asSinglePartitionBytes, 0, index)
      val singlePartitionSet = new java.util.HashMap[String, Int]()
      singlePartitionResultStr.split(",").filter(_.nonEmpty)
        .foreach(content => {
          if (singlePartitionSet.containsKey(content)) {
            singlePartitionSet.put(content, singlePartitionSet.get(content) + 1)
          } else {
            singlePartitionSet.put(content, 1)
          }
        })

      inputStream.close()
      assert(resultSet.size() == singlePartitionSet.size())
      assert(singlePartitionSet.values().asScala.count(_ > 1) == 0)

      shuffleClient.shutDown()
    }
  }
}
