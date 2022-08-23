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

import java.io.File
import java.util
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import com.bytedance.css.common.protocol.{PartitionInfo, ShuffleMode, WorkerAddress}
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.util.Utils

class BatchPushDataSuite extends LocalClusterSuite {

  val rand = new Random()

  test("batch pushData success") {
    Seq(ShuffleMode.DISK, ShuffleMode.HDFS).foreach { shuffleMode =>
      val appId = s"appId-${System.currentTimeMillis()}"
      val shuffleId = 5

      val mapperId = 0
      val partitionInfo = new PartitionInfo(0, 0)

      val workers = Array(
        new WorkerAddress(worker(0).rpcEnv.address.host, worker(0).pushPort),
        new WorkerAddress(worker(1).rpcEnv.address.host, worker(1).pushPort))

      var fileLength = 0L
      val allfutureList = new util.ArrayList[CompletableFuture[_]]()
      val futuresList = new util.ArrayList[Array[CompletableFuture[Int]]]()
      (0 until 1000).foreach(index => {
        // send pushData to replica piece.
        val length: Int = 1 + rand.nextInt(16 * 1024)
        fileLength += length
        val tmp: Array[Byte] = new Array[Byte](length)
        val (allFuture, futures) = batchPushData(appId, shuffleId,
          Array(partitionInfo.getReducerId), partitionInfo.getEpochId, mapperId,
          tmp, Array(0, tmp.length), workers, shuffleMode.toString)
        allfutureList.add(allFuture)
        futuresList.add(futures)
      })
      allfutureList.asScala.foreach(_.get())
      assert(!futuresList.asScala.flatten.exists(_.get() != 0))

      val committed1 = sendWorkerRpc[CommitFiles, CommitFilesResponse](worker(0).rpcPort,
        CommitFiles(Utils.getShuffleKey(appId, shuffleId)))
      val replica1 = committed1.committed.asScala.head

      val committed2 = sendWorkerRpc[CommitFiles, CommitFilesResponse](worker(1).rpcPort,
        CommitFiles(Utils.getShuffleKey(appId, shuffleId)))
      val replica2 = committed2.committed.asScala.head

      var r1Path = replica1.getFilePath
      if (shuffleMode == ShuffleMode.HDFS) {
        r1Path = r1Path.split(":").drop(1).mkString(":")
      }
      assert(replica1.getReducerId == 0)
      assert(replica1.getEpochId == 0)
      assert(new File(r1Path).length() == replica1.getFileLength)

      var r2Path = replica2.getFilePath
      if (shuffleMode == ShuffleMode.HDFS) {
        r2Path = r2Path.split(":").drop(1).mkString(":")
      }
      assert(replica2.getReducerId == 0)
      assert(replica2.getEpochId == 0)
      assert(new File(r2Path).length() == replica2.getFileLength)

      assert(replica1.getFileLength == replica2.getFileLength)
    }
  }

  test("batch pushData empty/failed/giant mix") {
    Seq(ShuffleMode.DISK, ShuffleMode.HDFS).foreach { shuffleMode =>
      val appId = s"appId-${System.currentTimeMillis()}"
      val shuffleId = 10
      val mapperId = 0

      // don't write any data into empty partition.
      val empty = new PartitionInfo(0, 0)

      val workers = Array(
        new WorkerAddress(worker(0).rpcEnv.address.host, worker(0).pushPort),
        new WorkerAddress(worker(1).rpcEnv.address.host, worker(1).pushPort))

      // set some write exception when write data into replica.
      val failed = new PartitionInfo(0, 1)
      val failedFileLength = new AtomicLong(0L)

      val allfutureList = new util.ArrayList[CompletableFuture[_]]()
      val futuresList = new util.ArrayList[Array[CompletableFuture[Int]]]()
      (0 until 10).foreach(index => {
        // send pushData to all replica piece
        val length: Int = 1 + rand.nextInt(16 * 1024)
        failedFileLength.addAndGet(length)
        val tmp: Array[Byte] = new Array[Byte](length)
        val (allFuture, futures) = batchPushData(appId, shuffleId,
          Array(failed.getReducerId), failed.getEpochId, mapperId,
          tmp, Array(0, tmp.length), workers, shuffleMode.toString)
        allfutureList.add(allFuture)
        futuresList.add(futures)
      })
      allfutureList.asScala.foreach(_.get())
      assert(!futuresList.asScala.flatten.exists(_.get() != 0))
      allfutureList.clear()
      futuresList.clear()

      // try set Exception for failed partitionInfo with replica index 1.
      sendWorkerRpc[BreakPartition, BreakPartitionResponse](worker(1).rpcPort,
        BreakPartition(Utils.getShuffleKey(appId, shuffleId), failed.getReducerId, failed.getEpochId))
      (0 until 10).foreach(index => {
        val (allFuture, futures) = batchPushData(appId, shuffleId,
          Array(failed.getReducerId), failed.getEpochId, mapperId,
          new Array[Byte](1024), Array(0, 1024), workers, shuffleMode.toString)
        allfutureList.add(allFuture)
        futuresList.add(futures)
      })
      allfutureList.asScala.foreach(_.get())
      futuresList.asScala.foreach { futures =>
        assert(futures(0).get() == 0)
        assert(futures(1).get() != 0)
      }
      allfutureList.clear()
      futuresList.clear()

      // write data into replica with no exception.
      val giant = new PartitionInfo(0, 2)
      val giantFileLength = new AtomicLong(0L)

      (0 until 10).foreach(index => {
        // send pushData to all replica piece
        val length: Int = 8 * 1024 * 1024
        giantFileLength.addAndGet(length)
        val tmp: Array[Byte] = new Array[Byte](length)
        val (allFuture, futures) = batchPushData(appId, shuffleId,
          Array(giant.getReducerId), giant.getEpochId, mapperId,
          tmp, Array(0, tmp.length), workers, shuffleMode.toString)
        allfutureList.add(allFuture)
        futuresList.add(futures)
      })
      allfutureList.asScala.foreach(_.get())
      assert(!futuresList.asScala.flatten.exists(_.get() != 0))

      val committed1 = sendWorkerRpc[CommitFiles, CommitFilesResponse](worker(0).rpcPort,
        CommitFiles(Utils.getShuffleKey(appId, shuffleId)))
      val committed2 = sendWorkerRpc[CommitFiles, CommitFilesResponse](worker(1).rpcPort,
        CommitFiles(Utils.getShuffleKey(appId, shuffleId)))

      Seq(committed1, committed2).foreach(f => {
        f.committed.asScala.foreach(committed => {
          if (committed.getReducerId == empty.getReducerId && committed.getEpochId == empty.getEpochId) {
            assert(committed.getFileLength == 0)
          } else if (committed.getReducerId == failed.getReducerId && committed.getEpochId == failed.getEpochId) {
            // replica index 0 done, replica index 1 failed.
            if (committed.getPort == worker(1).fetchPort) {
              assert(committed.getFileLength == -1)
            } else {
              // because replica index 0 done still fine,
              // the batchPushData with 10 * 1024 after break partition should be able to see in replica index 0
              assert(committed.getFileLength == failedFileLength.get() + 10 * 1024)
            }
          } else if (committed.getReducerId == giant.getReducerId && committed.getEpochId == giant.getEpochId) {
            assert(committed.getFileLength == giantFileLength.get())
          } else {
            throw new Exception("should not reach here.")
          }
        })
      })
    }
  }
}
