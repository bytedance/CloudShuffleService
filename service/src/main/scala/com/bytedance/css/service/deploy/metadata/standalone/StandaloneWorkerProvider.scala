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

package com.bytedance.css.service.deploy.metadata.standalone

import scala.collection.JavaConverters._

import com.bytedance.css.common.CssConf
import com.bytedance.css.service.deploy.metadata.WorkerProvider
import com.bytedance.css.service.deploy.worker.WorkerInfo


class StandaloneWorkerProvider(cssConf: CssConf) extends WorkerProvider {

  private val workerTimeoutMs = CssConf.workerTimeoutMs(cssConf)

  override def timeoutWorkers(): Seq[WorkerInfo] = {
    val currentTime = System.currentTimeMillis()
    activeWorkers.values().asScala.filter(_.lastHeartbeat < currentTime - workerTimeoutMs).toSeq
  }
}
