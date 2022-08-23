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

package com.bytedance.css.service.deploy.metadata.zookeeper

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.util.{ThreadUtils, Utils}
import com.bytedance.css.service.deploy.metadata.ExternalShuffleMeta
import io.netty.util.internal.ConcurrentSet
import org.apache.zookeeper.CreateMode

class ZookeeperExternalShuffleMeta(val cssConf: CssConf) extends ExternalShuffleMeta with Logging {

  val zkClient = ZookeeperClient.build(cssConf)
  val appSet = new ConcurrentSet[String]()
  val shuffleSet = new ConcurrentSet[String]()
  val updateThread = ThreadUtils
    .newDaemonSingleThreadScheduledExecutor("ZookeeperExternalShuffleMeta-update-thread")

  val intervalMs = CssConf.extMetaKeepaliveIntervalMs(cssConf)

  updateThread.scheduleAtFixedRate(new Runnable() {
    override def run(): Unit = {
      appSet.asScala.foreach(appId => {
        try {
          val appPath = zkClient.getApplicationPath(appId)
          val timestamp = System.currentTimeMillis()
          if (zkClient.checkExists(appPath)) {
            zkClient.setData(appPath, timestamp.toString)
          }
        } catch {
          case ex: Exception =>
            logError(s"Update zk app node $appId failed.", ex)
        }
      })

      shuffleSet.asScala.foreach(shuffleKey => {
        try {
          val shufflePath = zkClient.getShufflePath(shuffleKey)
          val timestamp = System.currentTimeMillis()
          if (zkClient.checkExists(shufflePath)) {
            zkClient.setData(shufflePath, timestamp.toString)
          }
        } catch {
          case ex: Exception =>
            logError(s"Update zk shuffle node $shuffleKey failed.", ex)
        }
      })
    }
  }, 0, intervalMs, TimeUnit.MILLISECONDS)

  override def appCreated(appId: String): Unit = {
    appSet.add(appId)
    val appPath = zkClient.getApplicationPath(appId)
    val timestamp = System.currentTimeMillis()
    zkClient.create(appPath, timestamp.toString, mode = CreateMode.PERSISTENT)
    log.info(s"appCreated in $appPath with timestamp: $timestamp")
  }

  override def appRemoved(appId: String): Unit = {
    appSet.remove(appId)
    val appPath = zkClient.getApplicationPath(appId)
    if (zkClient.checkExists(appPath)) {
      zkClient.delete(appPath)
    }
    log.info(s"appRemoved in $appPath")
  }

  override def shuffleCreated(shuffleKey: String): Unit = {
    shuffleSet.add(shuffleKey)
    val shufflePath = zkClient.getShufflePath(shuffleKey)
    val timestamp = System.currentTimeMillis()
    zkClient.create(shufflePath, timestamp.toString, mode = CreateMode.PERSISTENT)
    log.info(s"shuffleCreated in $shufflePath with timestamp: $timestamp")
  }

  override def shuffleRemoved(shuffleKeys: Set[String]): Unit = {
    shuffleKeys.foreach(shuffleKey => {
      shuffleSet.remove(shuffleKey)
      val shufflePath = zkClient.getShufflePath(shuffleKey)
      if (zkClient.checkExists(shufflePath)) {
        zkClient.delete(shufflePath)
      }
      log.info(s"shuffleRemoved in $shufflePath")
    })
  }

  override def cleanupIfNeeded(): Unit = {
    // Clean shuffleSet for current APP
    shuffleSet.asScala.foreach(shuffleKey => {
      try {
        val shufflePath = zkClient.getShufflePath(shuffleKey)
        if (zkClient.checkExists(shufflePath)) {
          zkClient.delete(shufflePath)
        }
        logInfo(s"cleanupIfNeeded Delete zk shuffle node $shuffleKey")
      } catch {
        case ex: Exception =>
          logError(s"cleanupIfNeeded Delete zk shuffle node $shuffleKey failed.", ex)
      }
    })

    // Clean appSet for current APP
    appSet.asScala.foreach(appId => {
      try {
        val appPath = zkClient.getApplicationPath(appId)
        if (zkClient.checkExists(appPath)) {
          zkClient.delete(appPath)
        }
        logInfo(s"cleanupIfNeeded Delete zk app node $appId")
      } catch {
        case ex: Exception =>
          logError(s"cleanupIfNeeded Delete zk app node $appId failed.", ex)
      }
    })

    // just try to clean up safely.
    Utils.tryLogNonFatalError(() -> {
      updateThread.shutdownNow()
      zkClient.close()
    })
  }
}
