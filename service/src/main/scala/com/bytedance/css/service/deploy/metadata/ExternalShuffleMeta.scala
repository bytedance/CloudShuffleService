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

package com.bytedance.css.service.deploy.metadata

import com.bytedance.css.common.CssConf
import com.bytedance.css.service.deploy.metadata.WorkerRegistryFactory.{TYPE_STANDALONE, TYPE_ZOOKEEPER}
import com.bytedance.css.service.deploy.metadata.zookeeper.ZookeeperExternalShuffleMeta

/**
 * An external metadata hook which hold by master to inform outsider dependency to deal with following events.
 *
 * AppCreated
 * AppRemoved
 * ShuffleCreated
 * ShuffleRemoved
 */
trait ExternalShuffleMeta {

  def appCreated(appId: String): Unit

  def appRemoved(appId: String): Unit

  def shuffleCreated(shuffleKey: String): Unit

  def shuffleRemoved(shuffleKeys: Set[String]): Unit

  def cleanupIfNeeded(): Unit
}

class StandaloneExternalShuffleMeta extends ExternalShuffleMeta {
  // Standalone mode does not need to inform metadata to External
  override def appCreated(appId: String): Unit = {}
  override def appRemoved(appId: String): Unit = {}
  override def shuffleCreated(shuffleKey: String): Unit = {}
  override def shuffleRemoved(shuffleKeys: Set[String]): Unit = {}
  override def cleanupIfNeeded(): Unit = {}
}

object ExternalShuffleMeta {

  def create(cssConf: CssConf): ExternalShuffleMeta = {
    CssConf.workerRegistryType(cssConf) match {
      case TYPE_STANDALONE => new StandaloneExternalShuffleMeta()
      case TYPE_ZOOKEEPER => new ZookeeperExternalShuffleMeta(cssConf)
    }
  }
}
