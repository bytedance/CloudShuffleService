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

package com.bytedance.css.common.protocol

import java.util

import com.bytedance.css.common.rpc.RpcEndpointRef

sealed trait Message extends Serializable

object CssRpcMessage {

  /**
   * Rpc Message For Cluster Service With Master & Worker.
   */
  // sent from worker to master
  // register push port, fetch port
  // and additional rpcRef for master to call back
  case class RegisterWorker(
      name: String,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      workerRef: RpcEndpointRef) extends Message

  case class RegisterWorkerResponse(statusCode: CssStatusCode) extends Message

  case class HeartbeatFromWorker(
      name: String,
      rttAvgStat: Long,
      shuffleKeys: util.HashSet[String]) extends Message

  case class HeartbeatResponse(
      expiredShuffleKeys: util.HashSet[String],
      expiredAppIds: util.HashSet[String]) extends Message

  /**
   * Rpc Message For Master & App.
   */
  case class HeartbeatFromApp(appId: String) extends Message

  case class ApplicationLost(applicationId: String) extends Message

  /**
   * Rpc Message For Shuffle Flow Control Message.
   */
  // For test only, internal
  case class BreakPartition(shuffleKey: String, reducerId: Int, epochId: Int) extends Message
  case class BreakPartitionResponse(statusCode: CssStatusCode) extends Message

  case class RegisterPartitionGroup(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      maxPartitionsPerGroup: Int) extends Message

  case class RegisterPartitionGroupResponse(
      statusCode: CssStatusCode,
      partitionGroups: util.List[PartitionGroup]) extends Message

  case class ReallocatePartitionGroup(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      oldPartitionGroup: PartitionGroup) extends Message

  case class ReallocatePartitionGroupResponse(statusCode: CssStatusCode, partitionGroup: PartitionGroup) extends Message

  case class MapperEnd(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      epochList: util.List[PartitionInfo],
      batchBlacklist: util.List[FailedPartitionInfoBatch]) extends Message

  case class MapperEndResponse(statusCode: CssStatusCode) extends Message

  case class StageEnd(applicationId: String, shuffleId: Int) extends Message
  case class StageEndResponse(statusCode: CssStatusCode) extends Message

  case class CommitFiles(shuffleKey: String) extends Message
  case class CommitFilesResponse(committed: util.List[CommittedPartitionInfo]) extends Message

  case class CloseFile(shuffleKey: String, partition: PartitionInfo) extends Message
  case object CloseFileResponse extends Message

  case class GetReducerFileGroups(applicationId: String, shuffleId: Int) extends Message
  case class GetReducerFileGroupsResponse(
      status: CssStatusCode,
      fileGroup: Array[Array[CommittedPartitionInfo]],
      attempts: Array[Int],
      batchBlacklist: util.HashSet[FailedPartitionInfoBatch]) extends Message

  case class UnregisterShuffle(applicationId: String, shuffleId: Int) extends Message
  case class UnregisterShuffleResponse(statusCode: CssStatusCode) extends Message

}
