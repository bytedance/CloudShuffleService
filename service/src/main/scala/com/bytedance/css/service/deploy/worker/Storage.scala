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

import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator

import scala.sys.process._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.PartitionInfo
import com.bytedance.css.common.util.Utils
import com.bytedance.css.service.deploy.worker.Storage.workingDirName
import io.netty.util.internal.ConcurrentSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Storage is mainly used to create a file writer for writing.
 * and also used to clean up shuffle and app data
 */
trait Storage extends Logging {

  /**
   * Create fileWriter for writing shuffle data
   */
  @throws[IOException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      partitionInfo: PartitionInfo,
      replicaIndex: Int,
      rotateThreshold: Long): FileWriter

  def bufferTotal(): Int = {
    throw new UnsupportedOperationException
  }

  def bufferLoad(): Int = {
    throw new UnsupportedOperationException
  }

  def cleanupExpiredShuffle(expiredShuffleKey: String): Unit

  def cleanupExpiredApp(expiredAppId: String): Unit
}

object Storage {
  val workingDirName = "css-worker/data"
}

class DiskStorage(cssConf: CssConf) extends Storage {
  // common conf
  private val queueCapacity = CssConf.flushQueueCapacity(cssConf)
  private val flushBufferSize = CssConf.flushBufferSize(cssConf).toInt
  private val epochRotateThreshold = CssConf.epochRotateThreshold(cssConf)

  // disk flush conf
  private val fetchChunkSize = CssConf.fetchChunkSize(cssConf)
  private val timeoutMs = CssConf.flushTimeoutMs(cssConf)
  private val diskFlushNum = CssConf.diskFlusherNum(cssConf)

  private val diskWorkingDirs: Array[File] = {
    val baseDirs = CssConf.diskFlusherBaseDirs(cssConf).map(new File(_, workingDirName))
    baseDirs.foreach { dir =>
      try {
        dir.mkdirs()
        val file = new File(dir, s"_SUCCESS_${System.currentTimeMillis()}")
        file.createNewFile()
        file.delete()
      } catch {
        case ex: IOException =>
          logWarning(s"DiskFlusher base dir initialization check failed.", ex)
          throw ex;
      }
    }
    baseDirs
  }

  private val diskFlushers = {
    val flushNum = if (diskFlushNum != -1) diskFlushNum else diskWorkingDirs.length
    logInfo(s"created $flushNum diskFlusher.")
    (0 until flushNum).map(index => {
      new FileFlusherImpl(s"DiskFileFlusher-$index", FileFlusher.DISK_FLUSHER_TYPE, queueCapacity, flushBufferSize)
    })
  }

  private val diskCounter = new AtomicInteger()
  private val diskCounterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % diskWorkingDirs.length
  }

  private val diskFlusherCounter = new AtomicInteger()
  private val diskFlusherCounterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % diskFlushers.size
  }

  override def createWriter(
      appId: String,
      shuffleId: Int,
      partitionInfo: PartitionInfo,
      replicaIndex: Int,
      rotateThreshold: Long): FileWriter = {
    if (rotateThreshold < epochRotateThreshold) {
      logDebug(s"Rotate threshold set by user is lesser than server side," +
        s" use server config instead: $epochRotateThreshold")
    }
    val realThreshold = Math.max(rotateThreshold, epochRotateThreshold)

    val index = diskCounter.getAndUpdate(diskCounterOperator)
    val flusherIndex = diskFlusherCounter.getAndUpdate(diskFlusherCounterOperator)
    val shuffleDir = new File(diskWorkingDirs(index), s"$appId/$shuffleId")
    val mode = s"r$replicaIndex-data"
    val fileName = s"${partitionInfo.getReducerId}-${partitionInfo.getEpochId}-${mode}"
    val file = new File(shuffleDir, fileName)
    shuffleDir.mkdirs()
    val created = file.createNewFile()
    if (!created) {
      throw new IOException(s"File ${file.getAbsoluteFile} is already Exists.")
    }
    val writer = new DiskFileWriter(
      file,
      diskFlushers(flusherIndex),
      fetchChunkSize,
      timeoutMs,
      flushBufferSize,
      realThreshold
    )
    writer
  }

  override def cleanupExpiredShuffle(expiredShuffleKey: String): Unit = {
    val splits = expiredShuffleKey.split("-")
    val appId = splits.dropRight(1).mkString("-")
    val shuffleId = splits.last
    diskWorkingDirs.foreach { workingDir =>
      val deleteDirCommand = s"rm -rf $workingDir/$appId/$shuffleId/"
      logDebug(s"cleanupExpiredShuffleKey deleteDirCommand $deleteDirCommand")
      try {
        deleteDirCommand.!
      } catch {
        case ex: Exception =>
          logError(s"cleanupExpiredShuffleKey deleteDirCommand $deleteDirCommand failed.", ex)
      }
    }
  }

  override def cleanupExpiredApp(expiredAppId: String): Unit = {
    diskWorkingDirs.foreach { workingDir =>
      val deleteDirCommand = s"rm -rf $workingDir/$expiredAppId/"
      logDebug(s"cleanupExpiredAppDir deleteDirCommand $deleteDirCommand")
      try {
        deleteDirCommand.!
      } catch {
        case ex: Exception =>
          logError(s"cleanupExpiredAppDir deleteDirCommand $deleteDirCommand failed.", ex)
      }
    }
  }
}


class HdfsStorage(cssConf: CssConf) extends Storage {
  // common conf
  private val queueCapacity = CssConf.flushQueueCapacity(cssConf)
  private val flushBufferSize = CssConf.flushBufferSize(cssConf).toInt
  private val epochRotateThreshold = CssConf.epochRotateThreshold(cssConf)

  // hdfs flush conf
  private val timeoutMs = CssConf.flushTimeoutMs(cssConf)
  private val hdfsFlushNum = CssConf.hdfsFlusherNum(cssConf)
  private val hdfsFlusherBaseDir = CssConf.hdfsFlusherBaseDir(cssConf)

  // get hadoop configuration from css conf
  private lazy val hadoopConf: Configuration = {
    val tmpHadoopConf = new Configuration()
    for ((key, value) <- cssConf.getAll if key.startsWith("css.hadoop.")) {
      tmpHadoopConf.set(key.substring("css.hadoop.".length), value)
    }
    // set replica for writer files
    tmpHadoopConf.set("dfs.checksum.type", "NULL")
    tmpHadoopConf.set("dfs.replication", CssConf.hdfsFlusherReplica(cssConf))
    tmpHadoopConf
  }

  private lazy val fs: FileSystem = new Path(hdfsFlusherBaseDir).getFileSystem(hadoopConf)

  private val hdfsFlushers = {
    (0 until hdfsFlushNum).map(index => {
      new FileFlusherImpl(s"HdfsFileFlusher-$index", FileFlusher.HDFS_FLUSHER_TYPE, queueCapacity, flushBufferSize)
    })
  }

  private val hdfsCounter = new AtomicInteger()
  private val hdfsCounterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % hdfsFlushNum
  }

  // hold shuffle & app info which used hdfs storage
  private val hdfsShuffleSet = new ConcurrentSet[String]()
  private val hdfsAppSet = new ConcurrentSet[String]()

  override def createWriter(
      appId: String,
      shuffleId: Int,
      partitionInfo: PartitionInfo,
      replicaIndex: Int,
      rotateThreshold: Long): FileWriter = {
    if (rotateThreshold < epochRotateThreshold) {
      logDebug(s"Rotate threshold set by user is lesser than server side," +
        s" use server config instead: $epochRotateThreshold")
    }
    val realThreshold = Math.max(rotateThreshold, epochRotateThreshold)

    val index = hdfsCounter.getAndUpdate(hdfsCounterOperator)
    val mode = s"r$replicaIndex-data"
    val fileName = s"${partitionInfo.getReducerId}-${partitionInfo.getEpochId}-${mode}"
    val path = new Path(s"$hdfsFlusherBaseDir/$appId/$shuffleId/$fileName")
    val outputStream = fs.create(path, true)
    val writer = new HdfsFileWriter(
      fs,
      path,
      outputStream,
      hdfsFlushers(index),
      timeoutMs,
      flushBufferSize,
      realThreshold
    )

    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    hdfsShuffleSet.add(shuffleKey)
    hdfsAppSet.add(appId)
    writer
  }

  override def cleanupExpiredShuffle(expiredShuffleKey: String): Unit = {
    val splits = expiredShuffleKey.split("-")
    val appId = splits.dropRight(1).mkString("-")
    val shuffleId = splits.last

    // delete hdfs shuffle path
    if (hdfsShuffleSet.contains(expiredShuffleKey)) {
      val path = new Path(s"$hdfsFlusherBaseDir/$appId/$shuffleId")
      try {
        logDebug(s"cleanupExpiredHdfsShuffleKey path $path")
        fs.delete(path, true)
      } catch {
        case ex: Exception =>
          logError(s"cleanupExpiredHdfsShuffleKey path $path failed.", ex)
      }
      hdfsShuffleSet.remove(expiredShuffleKey)
    }
  }

  override def cleanupExpiredApp(expiredAppId: String): Unit = {
    // delete hdfs app path
    if (hdfsAppSet.contains(expiredAppId)) {
      val path = new Path(s"$hdfsFlusherBaseDir/$expiredAppId")
      try {
        logDebug(s"cleanupExpiredHdfsAppDir path $path")
        fs.delete(path, true)
      } catch {
        case ex: Exception =>
          logError(s"cleanupExpiredHdfsAppDir path $path failed.", ex)
      }
      hdfsAppSet.remove(expiredAppId)
    }
  }
}
