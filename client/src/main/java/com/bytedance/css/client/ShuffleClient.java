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

package com.bytedance.css.client;

import com.bytedance.css.client.impl.ShuffleClientImpl;
import com.bytedance.css.client.stream.CssInputStream;
import com.bytedance.css.client.stream.disk.CssRemoteDiskEpochReader;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.common.protocol.PartitionGroup;
import com.bytedance.css.common.protocol.PartitionGroupManager;
import com.bytedance.css.network.client.TransportClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ShuffleClient implements Cloneable {

  protected static volatile ShuffleClient shuffleClient;

  public static ShuffleClient get(CssConf cssConf) {
    if (shuffleClient == null) {
      synchronized (ShuffleClient.class) {
        if (shuffleClient == null) {
          shuffleClient = new ShuffleClientImpl(cssConf);
        }
      }
    }
    return shuffleClient;
  }

  // key: shuffleId
  // value: PartitionGroupManager(Map<reduceId, PartitionGroup>)
  public static final ConcurrentHashMap<Integer, PartitionGroupManager>
    shufflePartitionGroupMap = new ConcurrentHashMap<>();

  /**
   * Send PushData to CSS Worker.
   *
   * In this way, Mapper calls and sends the RegisterShuffle event if needed through this interface.
   * and the CSS Master does the lock processing
   *
   * pushData is responsible for registerShuffle (other engines) so need numMappers numPartitions.
   * Return how many bytes were sent, this interface should be asynchronous.
   */
  public abstract int[] batchPushData(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int[] reducerIdArray,
      byte[] data,
      int[] offsetArray,
      int[] lengthArray,
      int numMappers,
      int numPartitions,
      boolean skipCompress) throws IOException;

  /**
   * Send MapperEnd event to CSS Master.
   *
   * If it is an empty data shuffle, then there may be no RegisterShuffle at all.
   * so the bottom line is, our MapperEnd also needs to send numMappers.
   * this way we can finally count until StageEnd is triggered.
   */
  public abstract void mapperEnd(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int numMappers) throws IOException;

  /**
   * Mapper ClientSide State cleanup
   */
  public abstract void mapperClose(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId);

  /**
   * Send GetReducerFileGroups event to CSS Master.
   *
   * only query partition info, do not fetch data
   */
  public abstract List<CommittedPartitionInfo> getPartitionInfos(
      String applicationId,
      int shuffleId,
      int[] reduceIds,
      int startMapIndex,
      int endMapIndex) throws IOException;

  /**
   * use client's client factory to create epoch reader. used for other engines
   */
  public abstract CssRemoteDiskEpochReader createEpochReader(
      String applicationId, int shuffleId,
      List<CommittedPartitionInfo> partitions,
      CssConf conf) throws IOException;

  /**
   * read all partition data as an input stream
   *
   * if startMapIndex > endMapIndex, means current PartitionSpec is specially handled BY CSS.
   * It's mainly used for SkewJoin for AdaptiveExecution.
   * In this case, startMapIndex and endMapIndex have different meanings.
   * startMapIndex -> Total SkewPartition task num
   * endMapIndex -> SkewPartition task index start from 0
   */
  public abstract CssInputStream readPartitions(
      String applicationId,
      int shuffleId,
      int[] reduceIds,
      int startMapIndex,
      int endMapIndex) throws IOException;

  /**
   * After call getPartitionInfos method. the client had mapper attempts in memory.
   */
  public abstract int[] getMapperAttempts(int shuffleId);

  /**
   * Register shuffle with retry.
   *
   * Register shuffle to the master, the master will return the worker pair information
   * which allocated by the current shuffle.
   * this interface is suitable for different engines
   */
  public abstract List<PartitionGroup> registerPartitionGroup(
      String applicationId,
      int shuffleId,
      int numMappers,
      int numPartitions,
      int maxPartitionsPerGroup) throws IOException;

  /**
   * after register shuffle successful, put allocated partitions into global reducerPartitionMap
   */
  public void applyShufflePartitionGroup(int shuffleId, List<PartitionGroup> partitionGroups) {
    if (!shufflePartitionGroupMap.containsKey(shuffleId)) {
      shufflePartitionGroupMap.put(shuffleId, new PartitionGroupManager(partitionGroups));
    }
  }

  // For Test Only
  public static void cleanShuffle(int shuffleId) {
    shufflePartitionGroupMap.remove(shuffleId);
  }

  /**
   * ShuffleManager unregister shuffle API will be called among Driver and Executors.
   * Only Driver should respond to send UnregisterShuffle Event to CSS
   */
  public abstract void unregisterShuffle(
      String applicationId,
      int shuffleId,
      boolean isDriver);

  /**
   * Enable application reports its status to CSS Master
   */
  public abstract void registerApplication(String applicationId);

  /**
   * Called by ShuffleManager stop to clean up entire ShuffleClient
   */
  public abstract void shutDown();

  public abstract TransportClientFactory getClientFactory();
}
