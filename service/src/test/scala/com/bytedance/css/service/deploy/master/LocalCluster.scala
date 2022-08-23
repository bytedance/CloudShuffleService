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

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.bytedance.css.client.compress.CssCompressorFactory
import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.{RpcNameConstants, TransportModuleConstants, WorkerAddress}
import com.bytedance.css.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import com.bytedance.css.common.unsafe.Platform
import com.bytedance.css.common.util.Utils
import com.bytedance.css.network.TransportContext
import com.bytedance.css.network.buffer.NettyManagedBuffer
import com.bytedance.css.network.client.RpcResponseCallback
import com.bytedance.css.network.protocol.BatchPushDataRequest
import com.bytedance.css.network.server.NoOpRpcHandler
import com.bytedance.css.service.deploy.worker.Worker
import io.netty.buffer.Unpooled
import org.scalatest.{BeforeAndAfterAll, FunSuite}


trait LocalClusterSuite extends FunSuite with BeforeAndAfterAll with Logging {

  private val conf = new CssConf().set("css.local.chunk.fetch.enabled", "false")
  private val rpcEnv = RpcEnv.create("AnyClient", Utils.localHostName(), 0, conf, true)
  private val dataTransportConf = Utils.fromCssConf(conf, TransportModuleConstants.DATA_MODULE, 8)
  private val context = new TransportContext(dataTransportConf, new NoOpRpcHandler, true)
  protected val dataClientFactory = context.createClientFactory(Nil.asJava)

  override def beforeAll(): Unit = {
    super.beforeAll()
    startCluster()
  }

  override def afterAll() {
    stopCluster()
    super.afterAll()
  }

  // mock local cluster for master & worker.
  val testConf: Map[String, String] = Map(
    "css.hdfsFlusher.num" -> "1",
    "css.hdfsFlusher.base.dir" -> "file:///tmp/hdfs_css",
    "css.test.mode" -> "true")
  def clusterConf: Map[String, String] = Map()

  var masterThread: Thread = null
  def master(): Master = Master.master
  final def masterRef: RpcEndpointRef = master().self
  final def heartbeatRef: RpcEndpointRef = rpcEnv.setupEndpointRef(
    masterRef.address, RpcNameConstants.HEARTBEAT)

  var workerThreads: ArrayBuffer[Thread] = new ArrayBuffer[Thread]()
  def worker(index: Int): Worker = Worker.workers(index)

  // val portOffset = new Random(System.currentTimeMillis()).nextInt(1000)

  def workersInitPorts: Seq[(Int, Int, Int)] = {
    Seq(
      (0, 0, 0),
      (0, 0, 0),
      (0, 0, 0))
  }

  def actualWorkers: Seq[(Int, Int, Int)] = {
    workersInitPorts.indices.map(index => {
      (worker(index).rpcPort, worker(index).pushPort, worker(index).fetchPort)
    })
  }

  def startCluster(): Unit = {
    val exception = new AtomicReference[Exception]
    def checkException(): Unit = {
      val e = exception.get
      if (e != null) throw e
    }

    try {
      masterThread = new Thread() {
        override def run(): Unit = {
          val confParam = (testConf ++ clusterConf).flatMap(f => Array("--conf", f._1, f._2))
          try {
            Master.main(Array("--local-mode") ++ confParam)
          } catch {
            case e: Exception =>
              logError("start master failed: " + e.getCause, e)
              exception.set(e)
          }
        }
      }
      masterThread.start()
      while ((Master.master == null || Master.master.self == null) && exception.get() == null) {
        Thread.sleep(3000)
      }
      checkException()

      workersInitPorts.foreach(f => {
        val thread = new Thread() {
          override def run(): Unit = {
            val masterArgs = Map("css.master.address" -> masterRef.address.toCssURL)
            val confParam = (testConf ++ clusterConf ++ masterArgs).flatMap(f => Array("--conf", f._1, f._2))
            try {
              Worker.main(Array("--port", f._1.toString, "-pp", f._2.toString, "-fp", f._3.toString)
                ++ confParam ++ Array(masterRef.address.toCssURL))
            } catch {
              case e: Exception =>
                logError("start worker failed: " + e.getCause, e)
                exception.set(e)
            }
          }
        }
        workerThreads.append(thread)
      })
      workerThreads.foreach(_.start())
      Thread.sleep(5000)

      if (Worker.workers.size != workersInitPorts.size) {
        throw new RuntimeException(
          s"started worker size ${Worker.workers.size} not match target size ${workersInitPorts.size}")
      }
      checkException()
    } catch {
      case e: Exception =>
        logError("start cluster failed. ", e)
        try {
          stopCluster()
        } finally {
          throw e
        }
    }
  }

  def stopCluster(): Unit = {
    val exceptionMap = new ConcurrentHashMap[String, Exception]()
    Worker.workers.foreach { worker =>
      try {
        worker.stop()
        worker.rpcEnv.shutdown()
        worker.rpcEnv.awaitTermination()
      } catch {
        case e: Exception =>
          logError("stop cluster worker failed. " + worker, e)
          exceptionMap.put("worker failed: \n" + e.getMessage, e)
      }
    }
    Worker.workers.clear()
    workerThreads.foreach(_.stop())

    try {
      Master.master.stop()
      Master.master.rpcEnv.shutdown()
      Master.master.rpcEnv.awaitTermination()
    } catch {
      case e: Exception =>
        logError("stop cluster master failed. ", e)
        exceptionMap.put("master failed: \n" + e.getMessage, e)
    } finally {
      Master.master = null
      masterThread.stop()
    }

    // stop dummy rpc env for test
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()

    dataClientFactory.close()
    context.close()

    exceptionMap.asScala.foreach(f => {
      throw f._2
    })
    Thread.sleep(5000)
  }

  def sendWorkerRpc[T: ClassTag, K: ClassTag](port: Int, req: T): K = {
    val workerRef =
      rpcEnv.setupEndpointRef(new RpcAddress(Utils.localHostName(), port), RpcNameConstants.WORKER_EP)
    workerRef.askSync[K](req)
  }

  def addHeaderAndCompressWithSinglePartition(
      cssConf: CssConf,
      mapperId: Int,
      mapperAttemptId: Int,
      reducerId: Int,
      batchId: Int,
      originalBytes: Array[Byte]): Array[Byte] = {
    val (data, offsets) = addHeaderAndCompressWithMultiPartition(
      cssConf, mapperId, mapperAttemptId, Array(reducerId), Array(batchId),
      originalBytes, Array(0), Array(originalBytes.length))
    assert(data.length == offsets.last)
    data
  }

  def addHeaderAndCompressWithMultiPartition(
      cssConf: CssConf,
      mapperId: Int,
      mapperAttemptId: Int,
      reducerIdArray: Array[Int],
      partitionBatchIds: Array[Int],
      data: Array[Byte],
      offsetArray: Array[Int],
      lengthArray: Array[Int]): (Array[Byte], List[Int]) = {
    val factory = new CssCompressorFactory(cssConf)
    val compressor = factory.getCompressor
    val compositeByteBuf = Unpooled.compositeBuffer(reducerIdArray.length)
    val compressedOffsetList = new util.ArrayList[Int]()
    compressedOffsetList.add(0)
    for (i <- reducerIdArray.indices) {
      compressor.compress(data, offsetArray(i), lengthArray(i))
      val compressedTotalSize = compressor.getCompressedTotalSize
      val BATCH_HEADER_SIZE = 4 * 4
      val body = new Array[Byte](BATCH_HEADER_SIZE + compressedTotalSize)
      Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapperId)
      Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, mapperAttemptId)
      Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, partitionBatchIds(i))
      Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize)
      System.arraycopy(compressor.getCompressedBuffer, 0, body, BATCH_HEADER_SIZE, compressedTotalSize)
      compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(body))
      compressedOffsetList.add(compressedOffsetList.asScala.last + body.length)
    }
    val sendBytes = new Array[Byte](compressedOffsetList.asScala.last)
    compositeByteBuf.readBytes(sendBytes)
    assert(sendBytes.length == compressedOffsetList.asScala.last)
    (sendBytes, compressedOffsetList.asScala.toList)
  }

  def batchPushData(
      appId: String,
      shuffleId: Int,
      reducerIds: Array[Int],
      epochId: Int,
      mapperId: Int,
      data: Array[Byte],
      offsets: Array[Int],
      workers: Array[WorkerAddress],
      shuffleMode: String,
      epochRotateThreshold: Long = CssConf.epochRotateThreshold(conf)):
  (CompletableFuture[_], Array[CompletableFuture[Int]]) = {
    val futures = new Array[CompletableFuture[Int]](workers.length)
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    workers.indices.foreach { index =>
      val future = new CompletableFuture[Int]
      val buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(data))
      val client = dataClientFactory.createClient(workers(index).host, workers(index).port)
      val newPushData = new BatchPushDataRequest(shuffleKey, reducerIds, epochId, offsets, mapperId,
        index, shuffleMode, epochRotateThreshold.toString, System.currentTimeMillis(), buffer)
      client.batchPushData(newPushData, new RpcResponseCallback() {
        override def onSuccess(response: ByteBuffer): Unit = {
          future.complete(0)
        }

        override def onFailure(e: Throwable): Unit = {
          logError("batchPushData failure: ", e)
          future.complete(1)
        }
      })
      futures(index) = future
    }
    (CompletableFuture.allOf(futures: _*), futures)
  }
}
