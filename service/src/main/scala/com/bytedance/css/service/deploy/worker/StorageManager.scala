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
import java.util
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.PartitionInfo
import com.bytedance.css.common.util.Utils

/**
 * StorageManager to manage flusher and writer resource
 * Also it should take care of shuffle and application expired data cleanup
 */
final class StorageManager(cssConf: CssConf) extends Logging {

  private val diskStorage = new DiskStorage(cssConf)
  private val hdfsStorage = new HdfsStorage(cssConf)

  // key: shuffleKey
  // value: Map(fileName -> writer)
  private val fileWriters = new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

  @throws[IOException]
  def createFileWriter(
      appId: String,
      shuffleId: Int,
      partitionInfo: PartitionInfo,
      replicaIndex: Int,
      rotateThreshold: Long,
      isHdfsWriter: Boolean = false): FileWriter = {
    var writer: FileWriter = null
    if (isHdfsWriter) {
      writer = hdfsStorage.createWriter(appId, shuffleId, partitionInfo, replicaIndex, rotateThreshold)
    } else {
      writer = diskStorage.createWriter(appId, shuffleId, partitionInfo, replicaIndex, rotateThreshold)
    }
    val shuffleKey = Utils.getShuffleKey(appId, shuffleId)
    fileWriters.putIfAbsent(shuffleKey, new ConcurrentHashMap[String, FileWriter]())
    fileWriters.get(shuffleKey).put(writer.getFilePath, writer)
    writer
  }

  def getFileWriter(shuffleKey: String, filePath: String): FileWriter = {
    val shuffleFileMap = fileWriters.get(shuffleKey)
    if (shuffleFileMap != null) {
      shuffleFileMap.get(filePath)
    } else {
      null
    }
  }

  // cleaner task will try to clean up expired shuffle data and writer
  private val cleanupQueue = new LinkedBlockingQueue[ExpiredTask]
  private val cleanupThread = new Thread("Cleanup thread for expired shuffle and app") {
    override def run(): Unit = {
      while (true) {
        val expiredTask = cleanupQueue.take()
        try {
          expiredTask match {
            case ShuffleKeyExpiredTask(expiredShuffleKeys) =>
              cleanupExpiredShuffleKey(expiredShuffleKeys)
            case AppExpiredTask(expiredAppIds) =>
              cleanupExpiredAppDir(expiredAppIds)
            case _ =>
              logWarning(s"Ignored cleanup expired ${expiredTask.taskType}")
          }
        } catch {
          case ex: Exception =>
            logError(s"Failed to cleanup expired ${expiredTask.taskType}", ex)
        }
      }
    }
  }

  def start(): Unit = {
    cleanupThread.setDaemon(true)
    cleanupThread.start()
  }

  def close(): Unit = {
    fileWriters.asScala.foreach { entry =>
      if (entry != null && entry._2 != null) {
        entry._2.values().asScala.foreach { fileWriter =>
          Utils.tryLogNonFatalError(fileWriter.close())
          Utils.tryLogNonFatalError(fileWriter.destroy())
        }
      }
    }
  }

  def addCleanupExpiredTask(expiredTask: ExpiredTask): Unit = {
    cleanupQueue.put(expiredTask)
  }

  private def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      val writers = fileWriters.remove(shuffleKey)
      if (writers != null && !writers.isEmpty) {
        writers.values().asScala.foreach(_.destroy())
      }
      diskStorage.cleanupExpiredShuffle(shuffleKey)
      hdfsStorage.cleanupExpiredShuffle(shuffleKey)
    }
  }

  private def cleanupExpiredAppDir(expiredAppIds: util.HashSet[String]): Unit = {
    expiredAppIds.asScala.foreach(appId => {
      diskStorage.cleanupExpiredApp(appId)
      hdfsStorage.cleanupExpiredApp(appId)
    })
  }
}

case class ShuffleKeyExpiredTask(expiredShuffleKeys: util.HashSet[String]) extends ExpiredTask {
  override var taskType: String = "ShuffleKeyTask"
}

case class AppExpiredTask(expiredAppIds: util.HashSet[String]) extends ExpiredTask {
  override var taskType: String = "AppTask"
}

sealed trait ExpiredTask {
  var taskType: String
}
