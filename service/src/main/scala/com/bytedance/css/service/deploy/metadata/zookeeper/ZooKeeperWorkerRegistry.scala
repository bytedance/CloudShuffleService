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

import java.util
import java.util.concurrent.TimeUnit

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.protocol.WorkerStatus
import com.bytedance.css.common.rpc.RpcEnv
import com.bytedance.css.common.util.{JsonUtils, ThreadUtils, Utils}
import com.bytedance.css.service.deploy.metadata.WorkerRegistry
import com.bytedance.css.service.deploy.worker.{WorkerInfo, WorkerSource}
import com.bytedance.css.service.deploy.worker.handler.RecycleShuffleHandler
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.zookeeper.{CreateMode, KeeperException}

class ZooKeeperWorkerRegistry(
    val rpcEnv: RpcEnv,
    val cssConf: CssConf,
    val handler: RecycleShuffleHandler) extends WorkerRegistry with Logging{

  val zkClient = ZookeeperClient.build(cssConf)
  var workerName: Option[String] = None

  private val expiredMetaMs = CssConf.extMetaExpireIntervalMs(cssConf)
  private val zkMaxThreads = CssConf.zkMaxParallelism(cssConf)
  private val expiredLogEnabled = CssConf.zkMetaExpiredLogEnable(cssConf)
  private lazy val lock = new InterProcessMutex(zkClient.curatorClient(), zkClient.lockPath)
  private val metaCleanupScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zkExpiredMetaCleanup-scheduler")

  override def toString: String = zkClient.toString

  override def register(workerInfo: WorkerInfo): Unit = {
    if (workerName.isEmpty) {
      workerName = Some(workerInfo.name)
    }

    // initial zk WorkerStatus with empty load
    val workerPath = zkClient.getWorkPath(workerInfo.name)
    val jsonStatus = JsonUtils.serialize(WorkerInfo.toWorkerStatus(workerInfo, 0))

    try {
      if (zkClient.checkExists(workerPath)) {
        zkClient.delete(workerPath)
      }
      zkClient.create(workerPath, jsonStatus, mode = CreateMode.PERSISTENT)
      // enable watch on app & shuffle removable events
      watchAppShuffleExpired()
    } catch {
      case ex: Exception =>
        logError(s"Zookeeper worker registry failed, client: $toString .", ex)
        System.exit(1)
    }
    metaCleanupScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        zkExpiredMetaCleanup()
      }
    }, 0, expiredMetaMs / 6, TimeUnit.MILLISECONDS)
  }

  private def watchAppShuffleExpired(): Unit = {
    logInfo(s"Start watching ${zkClient.applicationsPath} and ${zkClient.shufflesPath} .")

    // watch application expired
    zkClient.watchChild(zkClient.applicationsPath,
      event => if (event.getType == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
      logInfo(s"Application expired, watch path ${event.getData.getPath}, watch event ${event.getType}")
      val expiredApplication = event.getData.getPath.split("/").last
      handler.recycleApplication(new util.HashSet[String]() { add(expiredApplication) })
    })

    // watch shuffle expired
    zkClient.watchChild(zkClient.shufflesPath,
      event => if (event.getType == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
      logInfo(s"ShuffleKey expired, watch path ${event.getData.getPath}, watch event ${event.getType}")
      val expiredShuffle = event.getData.getPath.split("/").last
      handler.recycleShuffle(new util.HashSet[String]() { add(expiredShuffle) })
    })
  }

  private def zkExpiredMetaCleanup(): Unit = {
    val current = System.currentTimeMillis()
    if (lock.acquire(15, TimeUnit.SECONDS)) {
      // try remove other shuffle and app zk node from other application
      try {
        if (!zkClient.checkExists(zkClient.expiredTimePath) ||
          current - zkClient.getData(zkClient.expiredTimePath).toLong > 120000L) {
          logInfo("zkExpiredMetaCleanup executing .")
        } else {
          return
        }

        val shuffles = zkClient.list(zkClient.shufflesPath)
        val apps = zkClient.list(zkClient.applicationsPath)
        val workers = zkClient.list(zkClient.workersPath)

        ThreadUtils.parmap(
          workers, "worker cleanup", zkMaxThreads) { name =>
          try {
            val workerPath = zkClient.getWorkPath(name)
            val json = zkClient.getData(workerPath)
            val status = JsonUtils.deserialize(json, classOf[WorkerStatus])
            if (current - status.lastHeartbeat > expiredMetaMs) {
              val meter = WorkerSource.workerSource.WORKER_LOST_EVENT
              logInfo(s"Detect worker lost ${status.name} via zk expired")
              meter.mark(1L)
              // After the meter worker lost is issued, it cannot be kept forever,
              // so it needs to decrease by 1L after 90s to avoid constant count.
              metaCleanupScheduler.schedule(new Runnable {
                override def run(): Unit = {
                  meter.mark(-1L)
                  logInfo(s"Recover worker lost event ${status.name} -1L")
                }
              }, 90, TimeUnit.SECONDS)
              zkClient.delete(workerPath)
            }
          } catch {
            case ex: Exception =>
              if (expiredLogEnabled) {
                logError(s"remove expired worker failed with $name", ex)
              }
          }
        }

        ThreadUtils.parmap(
          shuffles, "other shuffles", zkMaxThreads) { shuffleKey =>
          try {
            val shufflePath = zkClient.getShufflePath(shuffleKey)
            val timestamp = zkClient.getData(shufflePath).toLong
            if (current - timestamp > expiredMetaMs) {
              zkClient.delete(shufflePath)
            }
          } catch {
            case ex: Exception =>
              if (expiredLogEnabled) {
                logError(s"remove other shuffle failed with $shuffleKey", ex)
              }
          }
        }

        ThreadUtils.parmap(
          apps, "other apps", zkMaxThreads) { appId =>
          try {
            val appPath = zkClient.getApplicationPath(appId)
            val timestamp = zkClient.getData(appPath).toLong
            if (current - timestamp > expiredMetaMs) {
              zkClient.delete(appPath)
            }
          } catch {
            case ex: Exception =>
              if (expiredLogEnabled) {
                logError(s"remove other app failed with $appId", ex)
              }
          }
        }

        // update meta expired processed time
        if (zkClient.checkExists(zkClient.expiredTimePath)) {
          zkClient.setData(zkClient.expiredTimePath, current.toString)
        } else {
          zkClient.create(zkClient.expiredTimePath, current.toString, CreateMode.PERSISTENT)
        }
      } catch {
        case _: Exception =>
          logWarning(s"zkExpiredMetaCleanup Delete other app shuffle node failed.")
      } finally {
        lock.release()
      }
    }
  }

  override def update(workerInfo: WorkerInfo, rttAvgStat: Long): Unit = synchronized {
    try {
      val status = WorkerInfo.toWorkerStatus(workerInfo, rttAvgStat)
      val workerPath = zkClient.getWorkPath(workerInfo.name)
      val jsonStatus = JsonUtils.serialize(status)
      zkClient.setData(workerPath, jsonStatus)
    } catch {
      case nne: KeeperException.NoNodeException =>
        logError(s"Try update worker: ${workerInfo.name} but ZK Node is missing, abort worker process.", nne)
        System.exit(1)
      case ex: Exception =>
        logError(s"Try update worker: ${workerInfo.name} with rttAvgStat: $rttAvgStat failed.", ex)
    }
  }

  override def close(): Unit = {
    try {
      if (workerName.isDefined) {
        val workerPath = zkClient.getWorkPath(workerName.get)
        zkClient.delete(workerPath)
      }
      zkClient.close()
    } catch {
      case ex: Exception =>
        logError(s"Worker: ${workerName.get} close cleanup failed.", ex)
    }
  }
}
