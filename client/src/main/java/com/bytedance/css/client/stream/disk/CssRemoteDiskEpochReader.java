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

package com.bytedance.css.client.stream.disk;

import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.network.buffer.ManagedBuffer;
import com.bytedance.css.network.buffer.NettyManagedBuffer;
import com.bytedance.css.network.client.ChunkReceivedCallback;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.client.TransportClientFactory;
import com.bytedance.css.network.protocol.shuffle.BlockTransferMessage;
import com.bytedance.css.network.protocol.shuffle.OpenStream;
import com.bytedance.css.network.protocol.shuffle.StreamHandle;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CssRemoteDiskEpochReader {

  private static final Logger logger = LoggerFactory.getLogger(CssRemoteDiskEpochReader.class);

  private CssConf cssConf;
  private TransportClient client;
  private TransportClientFactory clientFactory;

  private final boolean chunkFetchRetryEnable;
  private final int chunkFetchFailedRetryMaxTimes;
  private final long chunkRetryWaitIntervalMs;
  private int chunkFetchRetryTimes;

  private final String shuffleKey;
  private final long timeoutMs;
  private final int maxInFlight;

  private volatile CommittedPartitionInfo active;
  private final Iterator<CommittedPartitionInfo> iterator;

  private long streamId;
  private int numChunks;

  private int returnedChunks;
  private int chunkIndex;

  private final LinkedBlockingQueue<ByteBuf> results;
  private ChunkReceivedCallback callback;

  private boolean closed = false;
  private final AtomicReference<IOException> exception = new AtomicReference<>();

  public CssRemoteDiskEpochReader(
      CssConf cssConf,
      TransportClientFactory clientFactory,
      String shuffleKey,
      CommittedPartitionInfo[] partitions) throws IOException {
    this.cssConf = cssConf;
    this.clientFactory = clientFactory;
    this.shuffleKey = shuffleKey;
    this.iterator = Arrays.asList(partitions).iterator();
    this.chunkFetchRetryEnable = CssConf.chunkFetchRetryEnable(cssConf);
    this.chunkFetchFailedRetryMaxTimes = CssConf.chunkFetchFailedRetryMaxTimes(cssConf);
    this.chunkRetryWaitIntervalMs = CssConf.chunkFetchRetryWaitTimes(cssConf);
    this.timeoutMs = CssConf.fetchChunkTimeoutMs(cssConf);
    this.maxInFlight = CssConf.fetchChunkMaxReqsInFlight(cssConf);

    while (iterator.hasNext()) {
      this.active = iterator.next();
      openStream(0);
      if (exception.get() == null) {
        break;
      }
    }
    if (exception.get() != null) {
      String err_msg = String.format("init all active committed partition %s for shuffleKey %s it still failed.",
        Arrays.asList(partitions), this.shuffleKey);
      throw new IOException(err_msg, exception.get());
    }

    // once fetch success, place ByteBuf into result queue
    results = new LinkedBlockingQueue<>();
    callback = new RetryingChunkFetchListener();
  }

  private class RetryingChunkFetchListener implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      synchronized (CssRemoteDiskEpochReader.this) {
        if (callback == this && exception.get() == null) {
          ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
          if (!closed) {
            buf.retain();
            results.add(buf);
          }
        } else {
          logger.warn("ignore chunk fetch result with shuffleKey {} epoch {} partition {} streamId {} chunkIndex {}." +
            "because this callback is old.", shuffleKey, active.getEpochKey(), active, streamId, chunkIndex);
        }
      }
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      synchronized (CssRemoteDiskEpochReader.this) {
        if (callback == this) {
          String err_msg =
            String.format("fetchChunk failed with shuffleKey %s epoch %s partition %s streamId %s chunkIndex %s." +
              "it will retry.", shuffleKey, active.getEpochKey(), active, streamId, chunkIndex);
          logger.warn(err_msg, e);
          exception.set(new IOException(err_msg, e));
        } else {
          logger.warn("ignore chunk fetch failed with shuffleKey {} epoch {} partition {} streamId {} chunkIndex {}." +
            "because this callback is old.", shuffleKey, active.getEpochKey(), active, streamId, chunkIndex);
        }
      }
    }
  }

  private void openStream(int initChunkIndex) {
    try {
      this.client = this.clientFactory.createClient(this.active.getHost(), this.active.getPort());
      // request encoding for fetch streamId and numChunks and initChunkIndex
      OpenStream openStream = new OpenStream(this.shuffleKey, this.active.getFilePath(), initChunkIndex);
      ByteBuffer response = this.client.sendRpcSync(openStream.toByteBuffer(), this.timeoutMs);
      StreamHandle streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
      streamId = streamHandle.streamId;
      numChunks = streamHandle.numChunks;
      this.exception.set(null);
    } catch (Exception e) {
      exception.set(new IOException(e));
      logger.warn(String.format("init partition stream error for shuffleKey %s epoch %s partition %s.",
        shuffleKey, active.getEpochKey(), active), e);
    }
  }

  private void tryFetchChunk() throws IOException, InterruptedException {
    while (exception.get() != null && chunkFetchRetryEnable) {
      boolean wait = false;
      synchronized (this) {
        if (chunkFetchRetryTimes < chunkFetchFailedRetryMaxTimes) {
          this.chunkFetchRetryTimes++;
          this.callback = new RetryingChunkFetchListener();
          safeClearResults();
          this.exception.set(null);
          this.chunkIndex = returnedChunks;
          wait = true;
          logger.info("chunk fetch retry with shuffleKey {} epoch {} partition {} so far {} times",
            shuffleKey, active.getEpochKey(), active, chunkFetchRetryTimes);
        } else if (iterator.hasNext()) {
          this.active = iterator.next();
          this.callback = new RetryingChunkFetchListener();
          safeClearResults();
          this.chunkFetchRetryTimes = 0;
          this.chunkIndex = 0;
          this.returnedChunks = 0;
          this.exception.set(null);
          logger.info("move to next commit file to chunk fetch retry with shuffleKey {} epoch {} partition {} " +
            "so far {} times", shuffleKey, active.getEpochKey(), active, chunkFetchRetryTimes);
        } else {
          throw new IOException("chunk fetch retry all active committed partition for shuffleKey %s it still failed.");
        }
      }
      if (wait) {
        Thread.sleep(chunkRetryWaitIntervalMs);
      }
      // if chunk retry start from returnedChunks. if file retry start from 0.
      openStream(returnedChunks);
    }

    // here only check no retry and throw exception.
    // for retry times, skip this and let it retry fill max times.
    if (exception.get() != null && !chunkFetchRetryEnable) {
      throw exception.get();
    } else if (chunkIndex < numChunks) {
      fetchChunks();
    }
  }

  private void safeClearResults() {
    if (results.size() > 0) {
      results.forEach(res -> res.release());
    }
    results.clear();
  }

  public void close() throws IOException {
    synchronized(this) {
      closed = true;
    }
    safeClearResults();
  }

  public boolean hasNext() {
    return returnedChunks < numChunks;
  }

  public ByteBuf next() throws IOException {
    ByteBuf chunk = null;
    try {
      while (chunk == null) {
        tryFetchChunk();
        chunk = results.poll(500, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
    returnedChunks++;
    return chunk;
  }

  private void fetchChunks() {
    final int inFlight = chunkIndex - returnedChunks;
    if (inFlight < maxInFlight) {
      final int toFetch = Math.min(maxInFlight - inFlight + 1, numChunks - chunkIndex);
      for (int i = 0; i < toFetch; i++) {
        client.fetchChunk(streamId, chunkIndex++, callback);
      }
    }
  }

}
