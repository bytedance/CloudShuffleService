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

package com.bytedance.css.service.deploy.worker.handler;

import com.bytedance.css.common.exception.CssException;
import com.bytedance.css.network.client.RpcResponseCallback;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.protocol.shuffle.BlockTransferMessage;
import com.bytedance.css.network.protocol.shuffle.OpenStream;
import com.bytedance.css.network.protocol.shuffle.StreamHandle;
import com.bytedance.css.network.server.*;
import com.bytedance.css.network.util.TransportConf;
import com.bytedance.css.service.deploy.worker.WorkerSource;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FetchDataRpcHandler extends RpcHandler {

  private static final Logger logger = LoggerFactory.getLogger(FetchDataRpcHandler.class);

  private final TransportConf conf;
  private final FetchDataHandler handler;
  private final OneForOneStreamManager streamManager;
  private final FetchMetrics metrics;

  public FetchDataRpcHandler(TransportConf conf, FetchDataHandler handler) {
    this.conf = conf;
    this.handler = handler;
    streamManager = new OneForOneStreamManager();
    metrics = new FetchMetrics();
    streamManager.chunkFetchMetrics = metrics;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msg = BlockTransferMessage.Decoder.fromByteBuffer(message);
    OpenStream openStream = (OpenStream) msg;
    String shuffleKey = openStream.shuffleKey;
    String filePath = openStream.filePath;
    int chunkIndex = openStream.initChunkIndex;
    CssFileInfo fileInfo = handler.handleOpenStreamRequest(shuffleKey, filePath);
    if (fileInfo != null) {
      try {
        CssManagedBufferIterator iterator = new CssManagedBufferIterator(fileInfo, conf);
        iterator.setInitIndex(chunkIndex);
        long streamId = streamManager.registerStream(client.getClientId(), iterator, client.getChannel());
        streamManager.setStreamStateCurIndex(streamId, chunkIndex);

        if (fileInfo.numChunks == 0) {
          logger.warn(String.format("Zero numChunks for shuffle %s filePath %s", shuffleKey, filePath));
        }
        StreamHandle streamHandle = new StreamHandle(streamId, fileInfo.numChunks);
        callback.onSuccess(streamHandle.toByteBuffer());
      } catch (IOException ex) {
        String errorMsg = String.format("OpenStream failed for shuffle %s filePath %s", shuffleKey, filePath);
        logger.error(errorMsg, ex);
        callback.onFailure(new CssException(errorMsg, ex));
      }
    } else {
      callback.onFailure(new FileNotFoundException());
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    metrics.fetchConnection.inc();
    logger.debug("channel active " + client.getSocketAddress());
  }

  @Override
  public void channelInactive(TransportClient client) {
    metrics.fetchConnection.dec();
    logger.debug("channel Inactive " + client.getSocketAddress());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    logger.debug("exception caught " + cause + " " + client.getSocketAddress());
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /**
   * A simple class to wrap all fetch service wrapper metrics
   */
  private class FetchMetrics extends ChunkFetchMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    private final Counter fetchConnection = new Counter();

    // chunk fetch metrics
    private final Meter fetchDataQPS = new Meter();
    private final Timer fetchDataLatency = new Timer();
    private final Meter fetchDataThroughput = new Meter();
    private final Meter fetchDataFailed = new Meter();

    private FetchMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put(WorkerSource.workerSource().buildConnectionName("fetch"), fetchConnection);

      allMetrics.put(WorkerSource.workerSource().buildEventQPSName("FetchData"), fetchDataQPS);
      allMetrics.put(WorkerSource.workerSource().buildEventLatencyName("FetchData"), fetchDataLatency);
      allMetrics.put(WorkerSource.workerSource().buildDataThroughputName("FetchData"), fetchDataThroughput);
      allMetrics.put(WorkerSource.workerSource().buildEventFailedQPSName("FetchData"), fetchDataFailed);
    }

    @Override
    public Meter getChunkFetchQps() {
      return fetchDataQPS;
    }

    @Override
    public Timer getChunkFetchLatency() {
      return fetchDataLatency;
    }

    @Override
    public Meter getChunkFetchFailedQPS() {
      return fetchDataFailed;
    }

    @Override
    public Meter getChunkFetchThroughput() {
      return fetchDataThroughput;
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }
}
