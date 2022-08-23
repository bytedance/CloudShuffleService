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
import scala.util.Random

import com.bytedance.css.client.ShuffleClient
import com.bytedance.css.client.impl.ShuffleClientImpl
import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.CssRpcMessage.{GetReducerFileGroups, GetReducerFileGroupsResponse, RegisterPartitionGroup, RegisterPartitionGroupResponse}
import com.bytedance.css.common.protocol.CssStatusCode
import org.apache.commons.lang3.RandomStringUtils

class EpochRotateSuite extends LocalClusterSuite {

  // set cssConf as common, because ShuffleClient is singleton
  val cssConf = new CssConf()
  val random = new Random

  override def clusterConf: Map[String, String] = {
    Map("css.epoch.rotate.threshold" -> "4m")
  }

  Seq("DISK", "HDFS").foreach { mode =>
    test(s"epoch rotate assert $mode") {

      cssConf.set("css.master.address", masterRef.address.toCssURL)
      cssConf.set("css.local.chunk.fetch.enabled", "false")
      cssConf.set("css.epoch.rotate.threshold", "4m")
      cssConf.set("css.shuffle.mode", mode)

      val appId = s"appId-${System.currentTimeMillis()}"
      val shuffleId = 5
      val numMappers = 5
      val numPartitions = 1
      val maxPartitionsPerGroup = 1

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

      val resultSet = new util.HashMap[Int, util.HashSet[String]]()
      0.until(5).foreach { mapperId =>
        resultSet.put(mapperId, new util.HashSet[String]())
      }
      val actualLength = new util.HashMap[Int, Long]()
      0.until(5).foreach { mapperId =>
        actualLength.put(mapperId, 0L)
      }

      // just test retry to push data
      (0 until 4000).foreach { _ =>
        val mapperId = random.nextInt(4)
        val content = RandomStringUtils.randomAlphanumeric(1024, 2048);
        resultSet.get(mapperId).add(content)
        actualLength.put(mapperId, actualLength.get(mapperId) + content.length)
        val bytes = content.getBytes
        shuffleClient.batchPushData(appId, shuffleId, mapperId, mapperAttemptId,
          Array(reducerId), bytes, Array(0), Array(bytes.length), numMappers, numPartitions, false)
      }
      Thread.sleep(5000)

      (0 until 5).foreach { mapperId =>
        shuffleClient.mapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers)
      }

      Thread.sleep(2000)
      val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
        GetReducerFileGroups(appId, shuffleId))

      var maxEpoch = -1
      reducerFileGroupsRes.fileGroup.foreach(reducerFileGroupsRes => {
        reducerFileGroupsRes.foreach(p => {
          if (p.getEpochId > maxEpoch) {
            maxEpoch = p.getEpochId
          }
        })
      })

      assert(reducerFileGroupsRes.status == CssStatusCode.Success)
      assert(maxEpoch > 0)

      0.until(5).foreach { mapperId =>
        val inputStream = shuffleClient.readPartitions(appId, shuffleId, Array(0), mapperId, mapperId + 1)
        val resultBytes = new Array[Byte](actualLength.get(mapperId).toInt)
        var index = 0
        val buffer = new Array[Byte](1024)
        var readBytes = 0
        do {
          readBytes = inputStream.read(buffer, 0, 1024)
          if (readBytes > 0) {
            System.arraycopy(buffer, 0, resultBytes, index, readBytes)
            index += readBytes
          }
        } while (readBytes != -1)

        assert(actualLength.get(mapperId) == index)

        val resultString = new String(resultBytes)
        resultSet.get(mapperId).asScala.foreach(f => assert(resultString.indexOf(f) != -1))
        inputStream.close()
      }

      shuffleClient.shutDown()
    }
  }
}
