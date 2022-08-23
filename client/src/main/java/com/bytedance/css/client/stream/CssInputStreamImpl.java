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
import com.bytedance.css.client.compress.*;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CssInputStreamImpl extends CssInputStream {

  private static final Logger logger = LoggerFactory.getLogger(CssInputStreamImpl.class);

  private final EpochInputStreamBuilder inputStreamBuilder;

  private final String shuffleKey;
  private final int[] mapperAttemptIds;
  private final Set<String> failedBatchBlacklist;
  private final boolean failedBatchBlacklistEnable;
  private final int startMapIndex;
  private final int endMapIndex;
  private final boolean indexBasedSplitEnabled;

  private final FrameIterator frameIterator;
  private int position;
  private int limit;

  private Iterator<String> iterator = null;
  private String currentEpochKey;

  private byte[] decompressedBuf;
  private final CompressorFactory compressorFactory;
  private final Decompressor decompressor;

  private LinkedHashMap<String, ArrayList<CommittedPartitionInfo>> epochMap = new LinkedHashMap<>();
  private final Map<Integer, Set<Integer>> batchesRead = new HashMap<>();
  private MetricsCallback callback;
  private byte[] compressedBuf;

  public CssInputStreamImpl(
      CssConf conf,
      String shuffleKey,
      CommittedPartitionInfo[] partitions,
      EpochInputStreamBuilder inputStreamBuilder,
      int[] mapperAttemptIds,
      Set<String> failedBatchBlacklist,
      int startMapIndex,
      int endMapIndex) throws IOException {
    this.shuffleKey = shuffleKey;
    this.mapperAttemptIds = mapperAttemptIds;
    this.failedBatchBlacklist = failedBatchBlacklist;
    this.failedBatchBlacklistEnable = this.failedBatchBlacklist != null && this.failedBatchBlacklist.size() > 0;
    this.startMapIndex = startMapIndex;
    this.endMapIndex = endMapIndex;
    this.indexBasedSplitEnabled = startMapIndex > endMapIndex;

    this.inputStreamBuilder = inputStreamBuilder;
    for (int i = 0; i < partitions.length; i ++) {
      String key = partitions[i].getEpochKey();
      epochMap.putIfAbsent(key, new ArrayList<>());
      epochMap.get(key).add(partitions[i]);
    }

    iterator = shuffleEpochOrder(epochMap);

    // initialize length according to the push buffer size
    // if giant record met, recreate compressedBuf and decompressedBuf are necessary
    int blockInitLength = ((int)(CssConf.pushBufferSize(conf))) + CssCompressorTrait.HEADER_LENGTH;
    frameIterator = new FrameIterator();
    compressedBuf = new byte[blockInitLength];
    decompressedBuf = new byte[blockInitLength];
    this.compressorFactory = new CssCompressorFactory(conf);
    this.decompressor = this.compressorFactory.getDecompressor();

    // init first chunk for read()
    nextEpoch();
  }

  @Override
  public void setCallback(MetricsCallback callback) {
    this.callback = callback;
  }

  @Override
  public int read() throws IOException {
    if (position < limit) {
      int b = decompressedBuf[position];
      position++;
      return b & 0xFF;
    } else {
      if (!processBatch()) {
        return -1;
      } else {
        // if processBatch return true, position & limit will be set to valid index, call read() again.
        return read();
      }
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int readBytes = 0;
    while (readBytes < len) {
      while (position >= limit) {
        if (!processBatch()) {
          return readBytes > 0 ? readBytes : -1;
        }
      }

      int bytesToRead = Math.min(limit - position, len - readBytes);
      System.arraycopy(decompressedBuf, position, b, off + readBytes, bytesToRead);
      position += bytesToRead;
      readBytes += bytesToRead;
    }

    return readBytes;
  }

  @Override
  public void close() throws IOException {
    frameIterator.close();
  }

  // using bytes data in currentChunk
  // fetch HEADER: mapId + attemptId + batchId + size
  // and doing decompress to get origin bytes data which ShuffleWriter has been written.
  private boolean processBatch() throws IOException {

    long startTime = System.nanoTime();

    boolean hasData = false;
    while (hasNextFrame()) {
      Frame frame = frameIterator.next();

      int mapId = frame.getMapperId();
      int attemptId = frame.getAttemptId();
      int batchId = frame.getBatchId();
      int size = frame.getDataLength();

      if (compressedBuf.length < size) {
        compressedBuf = new byte[size];
      }
      frame.getData().readFully(compressedBuf, 0, size);

      // de-duplicate
      if (attemptId == mapperAttemptIds[mapId] &&
        (indexBasedSplitEnabled || (mapId >= startMapIndex && mapId < endMapIndex))) {
        if (failedBatchBlacklistEnable) {
          String blacklistKey = String.format("%s-%s-%s-%s", currentEpochKey, mapId, attemptId, batchId);
          if (failedBatchBlacklist.contains(blacklistKey)) {
            logger.warn("duplicated batch for failedBatchBlacklist: " + blacklistKey);
            continue;
          }
        }

        if (!batchesRead.containsKey(mapId)) {
          Set<Integer> batchSet = new HashSet<>();
          batchesRead.put(mapId, batchSet);
        }
        Set<Integer> batchSet = batchesRead.get(mapId);
        if (!batchSet.contains(batchId)) {
          batchSet.add(batchId);
          if (callback != null) {
            callback.incBytesRead(frame.getFrameLength());
          }
          // decompress data
          int originalLength = decompressor.getOriginalLen(compressedBuf);
          if (decompressedBuf.length < originalLength) {
            decompressedBuf = new byte[originalLength];
          }
          // TODO decompress data while read compress data could save byte array copy one time here
          limit = decompressor.decompress(compressedBuf, decompressedBuf, 0);
          position = 0;
          hasData = true;
          break;
        } else {
          logger.warn("duplicated batch: mapId " + mapId + ", attemptId "
            + attemptId + ", batchId " + batchId);
        }
      }
    }

    if (callback != null) {
      callback.incReadTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
    }
    return hasData;
  }

  private void nextEpoch() throws IOException {
    currentEpochKey = iterator.next();
    logger.info(String.format("Move to next Epoch %s for shuffle %s.", currentEpochKey, shuffleKey));
    ArrayList<CommittedPartitionInfo> partitions = epochMap.get(currentEpochKey);

    DataInputStream inputStream = inputStreamBuilder.createInputStream(partitions);

    if (inputStream == null) {
      String errMsg = String.format(
        "Failed to open all replica Epoch %s for shuffle %s.",currentEpochKey, shuffleKey);
      throw new IOException(errMsg);
    }
    frameIterator.resetInputStream(inputStream);
  }

  private boolean hasNextFrame() throws IOException {
    if (frameIterator.hasNext()) {
      return true;
    }
    if (iterator.hasNext()) {
      nextEpoch();
    }
    return frameIterator.hasNext();
  }
}
