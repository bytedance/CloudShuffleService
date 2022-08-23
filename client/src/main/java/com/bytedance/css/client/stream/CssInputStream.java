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

package com.bytedance.css.client.stream;

import com.bytedance.css.client.MetricsCallback;
import com.bytedance.css.client.stream.disk.EpochDiskInputStreamBuilder;
import com.bytedance.css.client.stream.hdfs.EpochHdfsInputStreamBuilder;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.common.protocol.PartitionInfo;
import com.bytedance.css.common.protocol.ShuffleMode;
import com.bytedance.css.network.client.TransportClientFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class CssInputStream extends InputStream {

  private static final Logger logger = LoggerFactory.getLogger(CssInputStream.class);

  // for spark metrics recording
  public abstract void setCallback(MetricsCallback callback);

  public static CssInputStream create(
      CssConf conf,
      TransportClientFactory clientFactory,
      String shuffleKey,
      CommittedPartitionInfo[] partitions,
      int[] mapperAttemptIds,
      Set<String> failedBatchBlacklist,
      int startMapIndex,
      int endMapIndex) throws IOException {

    CommittedPartitionInfo[] filterPartitions = partitions;

    if (startMapIndex > endMapIndex) {
      // if startMapIndex > endMapIndex
      // startMapIndex & endMapIndex can't be used as partition filter
      // since it's totally different meaning at this mode
      // let's see there are 0-0 0-1 0-2 0-3 0-4 0-5 0-6 0-7 0-8 8 epoch partition
      // and skew partition split to 5 task
      // task 0 should read 0-0 0-5
      // task 1 should read 0-1 0-6
      // task 2 should read 0-2 0-7
      // task 3 should read 0-3
      // task 4 should read 0-4
      // the task num might be bigger that epoch num for now.
      if (!CssConf.testMode(conf) && !CssConf.failedBatchBlacklistEnable(conf)) {
        throw new IOException(
          "SinglePartition split read mode need to set css.client.failed.batch.blacklist.enabled to true");
      }
      if (partitions != null && partitions.length != 0) {
        Set<CommittedPartitionInfo> sortSet = new TreeSet<>((o1, o2) -> {
          if (o1.getReducerId() > o2.getReducerId()) {
            return 1;
          } else if (o1.getReducerId() < o2.getReducerId()) {
            return -1;
          } else {
            return o1.getEpochId() - o2.getEpochId();
          }
        });
        sortSet.addAll(Arrays.asList(partitions));
        CommittedPartitionInfo[] orderedEpochKeys = sortSet.toArray(new CommittedPartitionInfo[0]);

        int startIndex = endMapIndex;
        int stepLen = startMapIndex;
        Set<CommittedPartitionInfo> assignedEpochKeys = new HashSet<>();
        while (startIndex < orderedEpochKeys.length) {
          assignedEpochKeys.add(orderedEpochKeys[startIndex]);
          startIndex += stepLen;
        }

        filterPartitions = assignedEpochKeys.toArray(new CommittedPartitionInfo[0]);

        logger.info("orderedEpochKeys {} s{} e{} taskIndex{} step{} assignedEpochKeys {} filterPartitions {}",
          StringUtils.join(Arrays.stream(orderedEpochKeys)
            .map(PartitionInfo::getEpochKey).toArray(String[]::new), ","),
              startMapIndex, endMapIndex, endMapIndex, stepLen,
          StringUtils.join(Arrays.stream(assignedEpochKeys.toArray(new CommittedPartitionInfo[0]))
            .map(PartitionInfo::getEpochKey).toArray(String[]::new), ","),
          StringUtils.join(Arrays.stream(filterPartitions)
            .map(PartitionInfo::getEpochKey).toArray(String[]::new), ","));

        if (partitions != null && filterPartitions != null && partitions.length != filterPartitions.length) {
          logger.info("After indexBasedSplitEnabled filtered, original {}, current {}",
            partitions.length, filterPartitions.length);
        }
      }
    }

    if (filterPartitions == null || filterPartitions.length == 0) {
      return emptyInputStream;
    } else {
      EpochInputStreamBuilder builder;
      if (filterPartitions[0].getShuffleMode() == ShuffleMode.DISK) {
        builder = new EpochDiskInputStreamBuilder(clientFactory, shuffleKey, conf);
      } else {
        FileSystem fs = new Path(filterPartitions[0].getFilePath()).getFileSystem(new Configuration());
        builder = new EpochHdfsInputStreamBuilder(fs);
      }
      return
        new CssInputStreamImpl(
          conf,
          shuffleKey,
          filterPartitions,
          builder,
          mapperAttemptIds,
          failedBatchBlacklist,
          startMapIndex,
          endMapIndex
        );
    }
  }

  public static Iterator<String> shuffleEpochOrder(
    LinkedHashMap<String, ArrayList<CommittedPartitionInfo>> epochMap) {
    // random shuffle for replica epoch
    epochMap.entrySet().stream().forEach(entry -> {
      Collections.shuffle(entry.getValue());
    });

    // random shuffle for epoch map
    List<String> shuffledEpochKeys = new ArrayList<>(epochMap.keySet());
    Collections.shuffle(shuffledEpochKeys);
    logger.info("After epochKey shuffle: {}", StringUtils.join(shuffledEpochKeys, ","));

    return shuffledEpochKeys.iterator();
  }

  public static CssInputStream empty() {
    return emptyInputStream;
  }

  private static final CssInputStream emptyInputStream = new CssInputStream() {
    @Override
    public int read() throws IOException {
      return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return -1;
    }

    @Override
    public void setCallback(MetricsCallback callback) {
    }
  };
}
