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

import com.bytedance.css.client.stream.CssInputStream
import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.{CssStatusCode, PartitionInfo, ShuffleMode}
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.util.Utils
import org.apache.commons.lang3.RandomStringUtils

class FetchDataSuite extends LocalClusterSuite {

  Seq(ShuffleMode.DISK, ShuffleMode.HDFS).foreach { mode =>
    Seq("lz4", "zstd").foreach { compressType =>
      Seq("true", "false").foreach { localChunkEnable =>
        test(s"push to single partition and read " +
          s"via InputStream $mode via compressType $compressType via localChunkEnable $localChunkEnable") {

          val cssConf = new CssConf()
          cssConf.set("css.compression.codec", compressType)
          cssConf.set("css.local.chunk.fetch.enabled", localChunkEnable)

          // registerShuffle
          val appId = s"appId-${System.currentTimeMillis()}"
          val shuffleId = 5
          val numMappers = 1
          val numPartitions = 1
          val maxPartitionsPerGroup = 1

          val mapperId = 0
          val mapperAttemptId = 0
          val reducerId = 0

          val res = masterRef.askSync[RegisterPartitionGroupResponse](
            RegisterPartitionGroup(appId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup)
          )
          assert(res.statusCode == CssStatusCode.Success)
          assert(res.partitionGroups.size() == 1)
          val partitionGroup = res.partitionGroups.asScala.head

          val resultSet = new java.util.HashSet[String]()

          var actualLength = 0L
          val allfutureList = new util.ArrayList[CompletableFuture[_]]()
          val futuresList = new util.ArrayList[Array[CompletableFuture[Int]]]()
          (0 until 1000).foreach(batchId => {
            val content = RandomStringUtils.randomAlphanumeric(1024, 2048)
            actualLength += content.length
            resultSet.add(content)
            val bytes = addHeaderAndCompressWithSinglePartition(
              cssConf, mapperId, mapperAttemptId, reducerId, batchId, content.getBytes)
            val (allFuture, futures) =
              batchPushData(appId, shuffleId, Array(reducerId), partitionGroup.epochId, mapperId,
                bytes, Array(0, bytes.length), partitionGroup.getReplicaWorkers.asScala.toArray, mode.toString)
            allfutureList.add(allFuture)
            futuresList.add(futures)
          })
          allfutureList.asScala.foreach(_.get())
          assert(!futuresList.asScala.flatten.exists(_.get() != 0))

          // send mapper end
          val epochList = new util.ArrayList[PartitionInfo]()
          epochList.add(new PartitionInfo(reducerId, partitionGroup.epochId))
          masterRef.askSync[MapperEndResponse](
            MapperEnd(appId, shuffleId, mapperId, mapperAttemptId, numMappers, epochList, null))

          Thread.sleep(5000)

          val reducerFileGroupsRes = masterRef.askSync[GetReducerFileGroupsResponse](
            GetReducerFileGroups(appId, shuffleId)
          )

          assert(reducerFileGroupsRes.status == CssStatusCode.Success)
          // filter two empty piece and a broken one
          assert(reducerFileGroupsRes.fileGroup.flatten.length == 2)
          // for partition 1 mapper attempt 4 comes first.
          reducerFileGroupsRes.attempts.zip(Array(0)).foreach(f => assert(f._1 == f._2))

          val inputStream = CssInputStream.create(cssConf, dataClientFactory, Utils.getShuffleKey(appId, shuffleId),
            reducerFileGroupsRes.fileGroup.head, reducerFileGroupsRes.attempts, null, 0, 1
          )

          var resultCount = 0L
          while (inputStream.read() != -1) {
            resultCount += 1
          }
          assert(actualLength == resultCount)
          inputStream.close()

          val inputStream2 = CssInputStream.create(cssConf, dataClientFactory, Utils.getShuffleKey(appId, shuffleId),
            reducerFileGroupsRes.fileGroup.head, reducerFileGroupsRes.attempts, null, 0, 1
          )
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
        }
      }
    }
  }
}
