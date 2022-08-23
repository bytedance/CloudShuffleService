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

package com.bytedance.css.service.deploy.worker

import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.Random
import java.util.concurrent._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.exception.{AlreadyClosedException, EpochShouldRotateException}
import com.bytedance.css.common.exception.PartitionInfoNotFoundException
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.metrics.MetricsSystem
import com.bytedance.css.common.protocol._
import com.bytedance.css.common.protocol.CssRpcMessage._
import com.bytedance.css.common.rpc._
import com.bytedance.css.common.util.{ThreadUtils, Utils}
import com.bytedance.css.network.TransportContext
import com.bytedance.css.network.buffer.NettyManagedBuffer
import com.bytedance.css.network.client.{RpcResponseCallback, TransportClientBootstrap}
import com.bytedance.css.network.protocol.BatchPushDataRequest
import com.bytedance.css.network.server.{CssFileInfo, TransportServerBootstrap}
import com.bytedance.css.service.deploy.common.ScheduledManager
import com.bytedance.css.service.deploy.metadata.WorkerRegistryFactory
import com.bytedance.css.service.deploy.metadata.WorkerRegistryFactory.{TYPE_STANDALONE, TYPE_ZOOKEEPER}
import com.bytedance.css.service.deploy.worker.handler._
import com.bytedance.css.service.deploy.worker.handler.PushDataRpcHandler.WrapPushMetricsCallBack
import io.netty.buffer.ByteBuf
import io.netty.util.internal.ConcurrentSet
import org.apache.hadoop.util.ShutdownHookManager

class Worker(
    override val rpcEnv: RpcEnv,
    val specificPushPort: Int,
    val specificFetchPort: Int,
    val conf: CssConf)
  extends RpcEndpoint with PushDataHandler with FetchDataHandler with RecycleShuffleHandler with Logging {

  private val host = rpcEnv.address.host
  val rpcPort = rpcEnv.address.port

  private val metricsSystem = MetricsSystem.createMetricsSystem(MetricsSystem.WORKER, conf)
  private val workerSource = WorkerSource.create(CssConf.clusterName(conf), rpcEnv.address.host)

  private val (pushServer, pushRpcHandler, pushTransportContext) = {
    val numThreads = CssConf.pushThreads(conf)
    val transportConf = Utils.fromCssConf(conf, TransportModuleConstants.PUSH_MODULE, numThreads)
    val pushRpcHandler = new PushDataRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, pushRpcHandler, false)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    (transportContext.createServer(CssConf.pushServerPort(conf, specificPushPort), serverBootstraps.asJava),
      pushRpcHandler, transportContext)
  }

  private val (fetchServer, fetchRpcHandler, fetchTransportContext) = {
    val numThreads = CssConf.fetchThreads(conf)
    val transportConf = Utils.fromCssConf(conf, TransportModuleConstants.FETCH_MODULE, numThreads)
    val fetchRpcHandler = new FetchDataRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, fetchRpcHandler, false)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    (transportContext.createServer(CssConf.fetchServerPort(conf, specificFetchPort), serverBootstraps.asJava),
      fetchRpcHandler, transportContext)
  }

  val pushPort = pushServer.getPort
  val fetchPort = fetchServer.getPort

  /**
   * TODO: Generate a better worker name
   * 1. Standalone mode: workerName = HostName
   * 2. Yarn mode: workerName = RoleName-${index}
   * 3. K8s mode: workerName = PodName-${index}
   */
  private val workerName = s"$host:$rpcPort:$pushPort:$fetchPort"
  private val workerInfo = new WorkerInfo(workerName, host, rpcPort, pushPort, fetchPort, self)

  private val storageManager = new StorageManager(conf)

  private val workerRegistry = WorkerRegistryFactory.create(rpcEnv, conf, this)

  private val scheduledManager = new ScheduledManager("worker-forward-message-scheduler", 2)

  // key: applicationId-shuffleId
  // value: Set(CommitFileStringFormat[ReducerId-EpochId])
  private val committedInfo = new ConcurrentHashMap[String, ConcurrentSet[String]]()

  // Threads & Pools
  private val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "for-commitFiles", CssConf.commitThreads(conf))

  // Only for test
  private val correctnessModeEnabled: Boolean = CssConf.correctnessModeEnable(conf)
  private val rand = new Random

  override def onStart(): Unit = {
    if (workerInfo.workerRpcRef == null) {
      workerInfo.workerRpcRef = self
    }
    workerRegistry.register(workerInfo)

    val updateIntervalMs = CssConf.workerRegistryType(conf) match {
      case TYPE_STANDALONE => CssConf.workerTimeoutMs(conf) / 4
      case TYPE_ZOOKEEPER => CssConf.workerUpdateIntervalMs(conf)
    }
    scheduledManager.addScheduledTask("updateWorkerStatusTask", () => {
      Utils.tryLogNonFatalError {
        val rttStat = 0L
        logDebug(s"Update rttStat ${rttStat} to zk node.")
        workerRegistry.update(workerInfo, rttStat)
      }
    }, 0, updateIntervalMs)

    val periodGcInterval = CssConf.cleanerPeriodicGCInterval(conf)
    scheduledManager.addScheduledTask("periodGcTask", () => {
      logDebug("periodGc start.")
      System.gc()
    }, periodGcInterval, periodGcInterval)

    scheduledManager.start()
    storageManager.start()

    if (!CssConf.testMode(conf)) {
      pushServer.registerCssMetrics(workerSource.cssMetricsPrefix)
      fetchServer.registerCssMetrics(workerSource.cssMetricsPrefix)
    }
    workerSource.registerMetricSet(pushServer.getAllCssMetrics)
    workerSource.registerMetricSet(fetchServer.getAllCssMetrics)
    workerSource.registerMetricSet(pushRpcHandler.getAllMetrics)
    workerSource.registerMetricSet(fetchRpcHandler.getAllMetrics)
    workerSource.registerMetricSet(FileWriterMetrics.instance)
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()

    // TODO add StorageManager buffer total count
    logInfo(s"Starting Worker $workerName")
  }

  override def onStop(): Unit = {
    logInfo(s"onStop called.")
    workerSource.WORKER_LOST_EVENT.mark()
    metricsSystem.report()
    logInfo(s"Detect worker lost ${workerInfo.name} via onStop")
    workerRegistry.close()
    storageManager.close()
    scheduledManager.stop()

    pushServer.close()
    pushTransportContext.close()

    fetchServer.close()
    fetchTransportContext.close()

    metricsSystem.stop()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req: BreakPartition =>
      handleBreakPartitionForTest(context, req: BreakPartition)
    case req: CommitFiles =>
      workerSource.withEventMetrics("CommitFiles") {
        handleCommitFiles(context, req: CommitFiles)
      }
    case req: CloseFile =>
      workerSource.withEventMetrics("CloseFile") {
        handleCloseFile(context, req: CloseFile)
      }
  }

  def handleBreakPartitionForTest(
      context: RpcCallContext,
      req: BreakPartition): Unit = {
    val partition = workerInfo.getShufflePartition(req.shuffleKey, req.reducerId, req.epochId)
    if (partition != null) {
      partition.asInstanceOf[WritablePartitionInfo].getFileWriter.setException()
    }
    context.reply(BreakPartitionResponse(CssStatusCode.Success))
  }

  def handleCloseFile(
      context: RpcCallContext,
      req: CloseFile): Unit = {
    val shuffleKey = req.shuffleKey
    val partition = workerInfo.getShufflePartition(shuffleKey, req.partition.getReducerId, req.partition.getEpochId)

    // try best to close rotate partitions
    // this forehead operation should decrease the time for stageEnd since we can pre-close some writer.
    commitThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          partition.asInstanceOf[WritablePartitionInfo].getFileWriter.close()
        } catch {
          case ex: Exception =>
            logError(s"CloseFile for shuffle $shuffleKey ${partition.getReducerId}-${partition.getEpochId} failed", ex)
        }
      }
    })

    context.reply(CloseFileResponse)
  }

  def handleCommitFiles(
      context: RpcCallContext,
      req: CommitFiles): Unit = {
    val shuffleKey = req.shuffleKey
    committedInfo.putIfAbsent(shuffleKey, new ConcurrentSet[String]())
    val futures = new util.ArrayList[Future[_]]()
    val committed = new util.ArrayList[CommittedPartitionInfo]()
    val partitions = workerInfo.getAllShufflePartitions(shuffleKey)

    if (partitions != null) {
      partitions.asScala.foreach { partition =>
        val future = commitThreadPool.submit(new Runnable {
          override def run(): Unit = {
            var fileWriter: FileWriter = null
            committedInfo.get(shuffleKey).add(partition.getEpochKey)
            try {
              fileWriter = partition.asInstanceOf[WritablePartitionInfo].getFileWriter
              val fileLength = fileWriter.close()
              committed.synchronized {
                committed.add(
                  // could be zero length file exists.
                  new CommittedPartitionInfo(partition.getReducerId, partition.getEpochId,
                    host, fetchPort, fileWriter.getShuffleMode, fileWriter.getFilePath, fileLength
                  )
                )
              }
            } catch {
              case ex: Exception =>
                logError(s"CommitFiles for shuffle $shuffleKey failed", ex)
                committed.synchronized {
                  committed.add(
                    new CommittedPartitionInfo(partition.getReducerId, partition.getEpochId,
                      host, fetchPort, fileWriter.getShuffleMode, null, -1
                    )
                  )
                }
            }
          }
        })
        futures.add(future)
      }
    }

    // wait for all fileWriter to commit
    val startMs = System.currentTimeMillis()
    futures.asScala.foreach(_.get(CssConf.stageEndTimeoutMs(conf), TimeUnit.MILLISECONDS))
    logInfo(s"CommitFiles for ${req.shuffleKey} for active num ${partitions.asScala.length} " +
      s"await ${System.currentTimeMillis() - startMs} ms")
    context.reply(CommitFilesResponse(committed))
  }

  private def writerLazyCreation(
     shuffleKey: String,
     reducerId: Int,
     epochId: Int,
     shuffleMode: String,
     replicaIndex: Int,
     epochRotateThreshold: String): PartitionInfo = synchronized {
    val current = workerInfo.getShufflePartition(shuffleKey, reducerId, epochId)
    if (current != null) {
      return current
    }

    if (shuffleMode.isEmpty ||
      epochRotateThreshold.isEmpty) {
      logError(s"create writer and partition from pushDataRequest, but extra header is empty. " +
        s"${shuffleMode} ${epochRotateThreshold}")
      return null
    }

    val splits = shuffleKey.split("-")
    val appId = splits.dropRight(1).mkString("-")
    val shuffleId = splits.last.toInt
    val p = Utils.toPartitionInfo(reducerId, epochId)

    try {
      val writer = storageManager.createFileWriter(
        appId, shuffleId, p, replicaIndex, epochRotateThreshold.toLong,
        ShuffleMode.valueOf(shuffleMode) == ShuffleMode.HDFS)
      val partitionInfo = new WritablePartitionInfo(p, writer)
      workerInfo.addShufflePartition(shuffleKey, partitionInfo)
      partitionInfo
    } catch {
      case ex: Exception =>
        logError(s"createWriter for $shuffleKey failed.", ex)
        null
    }
  }

  private def statusValidation(
    shuffleKey: String,
    reducerIds: Array[Int],
    epochIds: Array[Int],
    shuffleMode: String,
    replicaIndex: Int,
    epochRotateThreshold: String,
    ignoreRotate: Boolean,
    callback: RpcResponseCallback): FileWriterStatus = {

    require(reducerIds.length == epochIds.length, "length check")

    if (committedInfo.containsKey(shuffleKey)) {
      // Check for speculative task mapper end, inform that speculate task that partition is already committed
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](CssStatusCode.StageEnded.getValue)))
      return FileWriterStatus.UnWritable
    }

    // correctness random failed
    if (correctnessModeEnabled) {
      // 1/2000 chance to fail
      // epochId may fail when even numbered to prevent two consecutive failures
      if (rand.nextInt(2000) == 0 && epochIds.head % 2 == 0) {
        val msg = "correctnessModeEnabled is true, random failures"
        logWarning(msg)
        callback.onFailure(new Exception(msg))
        return FileWriterStatus.UnWritable
      }
    }

    reducerIds.indices.foreach { i =>
      var partitionInfo = workerInfo.getShufflePartition(shuffleKey, reducerIds(i), epochIds(i))
      if (partitionInfo == null) {
        partitionInfo = writerLazyCreation(shuffleKey, reducerIds(i), epochIds(i),
          shuffleMode, replicaIndex, epochRotateThreshold)
        if (partitionInfo == null) {
          val msg = s"${shuffleKey}-${reducerIds(i)}-${epochIds(i)} Lazy creation failed."
          logError(msg)
          callback.onFailure(new PartitionInfoNotFoundException(msg))
          return FileWriterStatus.UnWritable
        }
      }

      // check FileWriter status first and pass exception check
      var writer: FileWriter = null
      try {
        writer = partitionInfo.asInstanceOf[WritablePartitionInfo].getFileWriter
        writer.checkException()
        if (writer.shouldRotate()) {
          if (!ignoreRotate) {
            // ignoreRotate = false, directly notify the client that Rotate does not perform writing
            callback.onSuccess(ByteBuffer.wrap(Array[Byte](CssStatusCode.EpochShouldRotate.getValue)))
            return FileWriterStatus.ShouldRotate
          } else {
            // ignoreRotate = true, notify the client Rotate to execute the write after writing
            return FileWriterStatus.WritableButShouldRotate
          }
        }
      } catch {
        case ex: Exception =>
          callback.onFailure(ex)
          logError(s"statusValidation failed with shuffleKey ${shuffleKey}", ex)
          return FileWriterStatus.UnWritable
      }
    }
    FileWriterStatus.Writable
  }

  def writeBatchPushDataRequest(batchPushData: BatchPushDataRequest, callback: RpcResponseCallback): Boolean = {
    val buf = batchPushData.body().asInstanceOf[NettyManagedBuffer].getBuf

    try {
      batchPushData.reducerIds.indices.foreach { i =>
        val partitionInfo = workerInfo.getShufflePartition(
          batchPushData.shuffleKey,
          batchPushData.reducerIds(i),
          batchPushData.epochId
        )
        val writer: FileWriter = partitionInfo.asInstanceOf[WritablePartitionInfo].getFileWriter
        val byteBuf = buf.slice(
          buf.readerIndex() + batchPushData.offsets(i),
          batchPushData.offsets(i + 1) - batchPushData.offsets(i)
        )

        val isWritten = writePushDataRequest(
          batchPushData.shuffleKey,
          batchPushData.reducerIds(i),
          batchPushData.epochId,
          batchPushData.mapperId,
          byteBuf, writer, true, callback)
        if (!isWritten) {
          return false
        }
      }
      true
    } catch {
      case t: Throwable =>
        callback.onFailure(t)
        logError(s"writeBatchPushDataRequest failed with buf size ${buf.readableBytes()}", t)
        false
    }
  }

  override def handleBatchPushDataRequest(batchPushData: BatchPushDataRequest, callback: RpcResponseCallback): Unit = {
    // this callback will carry metrics.
    val wrappedCallback = callback

    val valid = statusValidation(
      batchPushData.shuffleKey,
      batchPushData.reducerIds,
      Array.fill(batchPushData.reducerIds.length)(batchPushData.epochId),
      batchPushData.shuffleMode,
      batchPushData.replicaIndex,
      batchPushData.epochRotateThreshold, true, wrappedCallback
    )

    // batchPushData ignoreRotate = true
    // can continue writing only when the return value is WritableButShouldRotate & Writable
    // writableButShouldRotate needs to specify the return value to notify the client
    if (valid != FileWriterStatus.Writable && valid != FileWriterStatus.WritableButShouldRotate) {
      return
    }

    val written = writeBatchPushDataRequest(batchPushData, wrappedCallback)
    if (written) {
      valid match {
        case FileWriterStatus.Writable =>
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
        case FileWriterStatus.WritableButShouldRotate =>
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](CssStatusCode.EpochShouldRotate.getValue)))
        case _ => throw new IOException("Wrong FileWriterStatus, should not reach here.")
      }
    } else {
      // If the callback has been called before, this callback will be ignored.
      wrappedCallback.onFailure(new Exception("Other Exception"))
    }
  }

  // return boolean value indicates whether need to do replication
  private def writePushDataRequest(
      shuffleKey: String,
      reducerId: Int,
      epochId: Int,
      mapperId: Int,
      byteBuf: ByteBuf,
      writer: FileWriter,
      ignoreRotate: Boolean,
      callback: RpcResponseCallback): Boolean = {
    // after fast ask, should continue to do write operation
    writer.incrementPendingWrites()
    try {
      writer.write(byteBuf, mapperId, ignoreRotate)
      callback match {
        case wrapPushMetricsCallBack: WrapPushMetricsCallBack =>
          wrapPushMetricsCallBack.getDataThroughputMetrics.mark(byteBuf.readableBytes())
        case _ =>
      }
      return true
    } catch {
      case _: AlreadyClosedException =>
        // Async write on both master and slave should not return failure logging only.
        // Could be speculative task still pushData to an cleaned shuffle partition
        // Silent passed for this case
        logWarning(s"Try to write into a closed/committed shuffle partition, ${shuffleKey} ${reducerId} ${epochId}")
      case _: EpochShouldRotateException =>
        // normally, when hit with exception, we use onFailure
        // but EpochRotation will be a high frequency operation in big shuffle
        // it will introduce some many error log if use onFailure API
        logError(s"Hit EpochShouldRotateException, ${shuffleKey} ${reducerId} ${epochId}")
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](CssStatusCode.EpochShouldRotate.getValue)))
      case ex: Throwable =>
        logError(s"Hit Unknown Exception, ${shuffleKey} ${reducerId} ${epochId}", ex)
        callback.onFailure(ex)
    }
    writer.decrementPendingWrites()
    false
  }

  override def handleOpenStreamRequest(shuffleKey: String, filePath: String): CssFileInfo = {
    // find FileWriter responsible for the data
    val fileWriter = storageManager.getFileWriter(shuffleKey, filePath)
    if (fileWriter == null) {
      return null
    } else {
      new CssFileInfo(fileWriter.getFile, fileWriter.getChunkOffsets, fileWriter.getFileLength)
    }
  }

  override def recycleShuffle(expiredShuffleKeys: util.HashSet[String]): Unit = {
    def cleanMetaData(expiredShuffleKeys: util.HashSet[String]): Unit = {
      expiredShuffleKeys.asScala.foreach { shuffleKey =>
        workerInfo.removeShufflePartitions(shuffleKey)
        committedInfo.remove(shuffleKey)
      }
    }
    cleanMetaData(expiredShuffleKeys)
    storageManager.addCleanupExpiredTask(ShuffleKeyExpiredTask(expiredShuffleKeys))
  }

  override def recycleApplication(expiredApplicationIds: util.HashSet[String]): Unit = {
    // since try to cleanup expired app. try to clean all shuffle with target appId first.
    expiredApplicationIds.asScala.foreach { appId =>
      val expiredShuffleKeys = workerInfo.getAllShuffleKeyByAppId(appId)
      if (!expiredShuffleKeys.isEmpty) {
        logInfo("try to clean up expired shuffle keys with no unregister shuffle. " + expiredShuffleKeys)
        recycleShuffle(new util.HashSet[String](expiredShuffleKeys))
      }
    }
    storageManager.addCleanupExpiredTask(AppExpiredTask(expiredApplicationIds))
  }
}

object Worker extends Logging {

  var workers: ArrayBuffer[Worker] = new ArrayBuffer[Worker]()

  def main(args: Array[String]): Unit = {
    val conf = new CssConf
    val workerArgs = new WorkerArguments(args, conf)

    if (!Utils.checkCssConfLegality(conf)) {
      System.exit(13)
    }

    val rpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs.bindHost,
      workerArgs.host,
      workerArgs.port,
      conf,
      0,
      false)

    val worker = new Worker(rpcEnv, workerArgs.pushPort, workerArgs.fetchPort, conf)
    workers.synchronized {
      workers.asJava.add(worker)
    }
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, worker)
    ShutdownHookManager.get().addShutdownHook(new Thread {
      override def run(): Unit = {
        logInfo(s"Worker shutting down, hook execution.")
        if (worker != null) {
          rpcEnv.shutdown()
        }
      }
    }, 50)
    rpcEnv.awaitTermination()
  }
}
