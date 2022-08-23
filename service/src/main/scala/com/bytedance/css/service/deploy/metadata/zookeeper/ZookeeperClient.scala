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

import java.io.Closeable

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.util.Utils
import org.apache.commons.lang3.StringUtils
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher}

class ZookeeperClient(val cssConf: CssConf) extends Closeable with Logging {
  import ZookeeperClient._

  private val servers: String = CssConf.zkAddress(cssConf)
  if (StringUtils.isBlank(servers)) {
    throw new IllegalArgumentException(s"Invalid config: servers=$servers")
  }
  private lazy val clusterName: String = {
    var activeClusterName = CssConf.clusterName(cssConf)
    if (StringUtils.isNotEmpty(CssConf.haClusterName(cssConf))) {
      activeClusterName = CssConf.haClusterName(cssConf)
      try {
        val haClusterPath = s"$clusterTagPath/${CssConf.haClusterName(cssConf)}"
        val tag = getData(haClusterPath)
        activeClusterName = s"${CssConf.haClusterName(cssConf)}${tag}"
      } catch {
        case t: Throwable =>
          val msg = s"error to get active tag for ha cluster ${activeClusterName}."
          logError(msg, t)
          throw new IllegalArgumentException(msg, t)
      }
    }
    if (StringUtils.isBlank(activeClusterName)) {
      throw new IllegalArgumentException(s"Invalid input: cluster=$activeClusterName")
    }
    logInfo(s"get current cluster name ${activeClusterName}")
    activeClusterName
  }

  private val retries: Int = CssConf.zkRetries(cssConf)
  private val client: CuratorFramework = CuratorFrameworkFactory.builder.connectString(servers)
    .sessionTimeoutMs(CssConf.zkSessionTimeoutMs(cssConf))
    .retryPolicy(new ExponentialBackoffRetry(MIN_RETRY_SLEEP_MS, retries, MAX_RETRY_SLEEP_MS))
    .build
  client.start()

  override def toString: String = {
    String.format("ZooKeeperServiceRegistry{servers=%s}", servers)
  }

  override def close(): Unit = {
    client.close()
  }

  def curatorClient(): CuratorFramework = client

  val clusterTagPath: String = s"/$ZK_BASE_PATH/$ZK_CLUSTER_TAG_PATH"

  val rootPath: String = s"/$ZK_BASE_PATH/$clusterName"

  val workersPath: String = s"$rootPath/$ZK_WORKER_SUBPATH"

  val applicationsPath: String = s"$rootPath/$ZK_APPLICATION_SUBPATH"

  val shufflesPath: String = s"$rootPath/$ZK_SHUFFLE_SUBPATH"

  val lockPath: String = s"$rootPath/$ZK_LOCK_SUBPATH"

  val expiredTimePath: String = s"$rootPath/$ZK_EXPIRED_TIME_SUBPATH"

  def getWorkPath(workerName: String): String = s"$workersPath/$workerName"

  def getApplicationPath(applicationId: String): String = s"$applicationsPath/$applicationId"

  def getShufflePath(applicationId: String, shuffleId: Int): String = {
    s"$shufflesPath/${Utils.getShuffleKey(applicationId, shuffleId)}"
  }

  def getShufflePath(shuffleKey: String): String = s"$shufflesPath/$shuffleKey"

  def checkExists(path: String): Boolean = {
    client.checkExists().forPath(path) != null
  }

  def create(path: String, value: String, mode: CreateMode = CreateMode.EPHEMERAL): Unit = {
    if (!checkExists(path)) {
      client.create.creatingParentsIfNeeded
        .withMode(mode).forPath(path, value.getBytes())
    }
  }

  def addStateListener(targetState: ConnectionState, func: () => Unit): Unit = {
    client.getConnectionStateListenable.addListener(new ConnectionStateListener {
      override def stateChanged(curatorFramework: CuratorFramework, connectionState: ConnectionState): Unit = {
        if (targetState == connectionState) {
          logInfo(s"ZooKeeper state changed: $targetState")
          func()
        }
      }
    })
  }

  def setData(path: String, value: String): Unit = {
    client.setData().forPath(path, value.getBytes())
  }

  def delete(path: String): Unit = {
    client.delete().forPath(path)
  }

  def getData(path: String): String = {
    new String(client.getData.forPath(path))
  }

  def list(path: String): Seq[String] = {
    client.getChildren.forPath(path).asScala
  }

  def watch(path: String, watchFun: WatchedEvent => Unit): Unit = {
    client.getData.usingWatcher(new Watcher {
      override def process(event: WatchedEvent): Unit = {
        watchFun(event)
      }
    }).forPath(path)
  }

  def watchChild(path: String, watchFun: PathChildrenCacheEvent => Unit): Unit = {
    val patchChildrenCache = new PathChildrenCache(client, path, true)
    patchChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE)
    patchChildrenCache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        watchFun(event)
      }
    })
  }
}

object ZookeeperClient {
  val ZK_BASE_PATH: String = "css"
  val ZK_CLUSTER_TAG_PATH: String = "tags"
  val ZK_WORKER_SUBPATH: String = "workers"
  val ZK_APPLICATION_SUBPATH: String = "applications"
  val ZK_SHUFFLE_SUBPATH: String = "shuffles"
  val ZK_LOCK_SUBPATH: String = "lock"
  val ZK_EXPIRED_TIME_SUBPATH: String = "expiredTime"
  val MIN_RETRY_SLEEP_MS: Int = 1000
  val MAX_RETRY_SLEEP_MS: Int = 10000

  @volatile var zkClient: ZookeeperClient = null

  def build(cssConf: CssConf): ZookeeperClient = synchronized {
    if (zkClient == null) {
      zkClient = new ZookeeperClient(cssConf)
    }
    zkClient
  }
}
