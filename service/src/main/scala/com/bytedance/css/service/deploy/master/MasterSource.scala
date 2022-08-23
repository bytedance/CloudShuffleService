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

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.service.deploy.common.BaseSource

class MasterSource(
    namespace: String,
    serverId: String)
  extends BaseSource(namespace, serverId) {

  override val sourceName = ""

  // RegisterPartitionGroup Qps & Latency
  val REGISTER_PARTITION_GROUP = getEventQPS("RegisterPartitionGroup")
  val REGISTER_PARTITION_GROUP_LATENCY = getEventLatency("RegisterPartitionGroup")

  // ReallocatePartitionGroup Qps & Latency
  val REALLOCATE_PARTITION_GROUP = getEventQPS("ReallocatePartitionGroup")
  val REALLOCATE_PARTITION_GROUP_LATENCY = getEventLatency("ReallocatePartitionGroup")

  // MapperEnd Qps & Latency
  val MAPPER_END = getEventQPS("MapperEnd")
  val MAPPER_END_LATENCY = getEventLatency("MapperEnd")

  // StageEnd Qps & Latency
  val STAGE_END = getEventQPS("StageEnd")
  val STAGE_END_LATENCY = getEventLatency("StageEnd")

  // GetReducerFileGroups Qps & Latency
  val GET_REDUCER_FILE_GROUPS = getEventQPS("GetReducerFileGroups")
  val GET_REDUCER_FILE_GROUPS_LATENCY = getEventLatency("GetReducerFileGroups")
}

object MasterSource extends Logging {

  @volatile var masterSource: MasterSource = _

  def create(namespace: String, serverId: String): MasterSource = synchronized {
    if (masterSource == null) {
      masterSource = new MasterSource(namespace, serverId)
      logInfo(s"${masterSource.getClass.getName} use cssMetricsPrefix ${masterSource.cssMetricsPrefix}")
    }
    masterSource
  }
}
