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

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.protocol.CssRpcMessage.{HeartbeatFromApp, RegisterPartitionGroup, RegisterPartitionGroupResponse, UnregisterShuffle, UnregisterShuffleResponse}
import com.bytedance.css.service.deploy.worker.Storage


class CleanupSuite extends LocalClusterSuite {

  val cssConf = new CssConf().set("css.maxPartitionsPerGroup", "1")

  override def clusterConf: Map[String, String] = {
    Map("css.remove.shuffle.delay" -> "3s",
      "css.app.timeout" -> "3s",
      "css.worker.timeout" -> "4s")
  }

  test("test application timeout") {

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 10
    heartbeatRef.send(HeartbeatFromApp(appId))

    val res = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, 10, 20, CssConf.maxPartitionsPerGroup(cssConf))
    )
    assert(res.partitionGroups.size() == 20)

    // sleep enough time to trigger app lost & unregister shuffle.
    Thread.sleep(15000)
    assert(!Master.master.getShuffleStageManager().validateRegisterShuffle(appId, shuffleId))
  }

  test("test unregister shuffle after delay ms") {

    val appId = s"appId-${System.currentTimeMillis()}"
    val shuffleId = 10
    val res = masterRef.askSync[RegisterPartitionGroupResponse](
      RegisterPartitionGroup(appId, shuffleId, 10, 20, CssConf.maxPartitionsPerGroup(cssConf))
    )
    assert(res.partitionGroups.size() == 20)

    masterRef.askSync[UnregisterShuffleResponse](UnregisterShuffle(appId, shuffleId))
    assert (Master.master.getShuffleStageManager().validateRegisterShuffle(appId, shuffleId))

    // sleep enough time to trigger app lost & unregister shuffle.
    Thread.sleep(6000)
    assert (!Master.master.getShuffleStageManager().validateRegisterShuffle(appId, shuffleId))
  }

  test("test cleanup application dir") {
    val dirs = CssConf.diskFlusherBaseDirs(cssConf).map(new File(_, Storage.workingDirName))
    val workingDir = dirs(0)
    workingDir.mkdirs()

    val size = 5
    val appIds = new Array[String](size)
    appIds.indices.foreach(index => {
      val appId = s"appId-${System.currentTimeMillis()}"
      Thread.sleep(100)
      appIds(index) = appId

      new File(workingDir, s"$appId").mkdirs()
    })

    appIds.indices.foreach(index => {
      heartbeatRef.send(HeartbeatFromApp(appIds(index)))
    })

    // must > 2 * css.app.timeout + css.remove.shuffle.delay + css.worker.timeout / 4
    Thread.sleep(10000)

    appIds.indices.foreach(index => {
      assert(! new File(workingDir, appIds(index)).exists())
    })
  }
}
