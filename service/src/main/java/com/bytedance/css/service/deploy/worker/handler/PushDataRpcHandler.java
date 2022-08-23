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

import com.bytedance.css.network.client.RpcResponseCallback;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.protocol.BatchPushDataRequest;
import com.bytedance.css.network.server.OneForOneStreamManager;
import com.bytedance.css.network.server.RpcHandler;
import com.bytedance.css.network.server.StreamManager;
import com.bytedance.css.network.util.TransportConf;
import com.bytedance.css.service.deploy.worker.WorkerSource;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PushDataRpcHandler extends RpcHandler {

  private static final Logger logger = LoggerFactory.getLogger(PushDataRpcHandler.class);

  private final TransportConf conf;
  private final PushDataHandler handler;
  private final OneForOneStreamManager streamManager;
  private final PushMetrics metrics;

  public PushDataRpcHandler(TransportConf conf, PushDataHandler handler) {
    this.conf = conf;
    this.handler = handler;
    streamManager = new OneForOneStreamManager();
    metrics = new PushMetrics();
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    throw new UnsupportedOperationException("PushDataRpcHandler");
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  @Override
  public void receiveBatchPushDataRequest(
      TransportClient client,
      BatchPushDataRequest req,
      RpcResponseCallback callback) {
    metrics.pushDataQPS.mark();
    final Timer.Context latencyTimer = metrics.pushDataLatency.time();
    try {
      handler.handleBatchPushDataRequest(req, new WrapPushMetricsCallBack(metrics, callback));
    } finally {
      metrics.pushDataE2ELatency.update(System.currentTimeMillis() - req.clientStartTime, TimeUnit.MILLISECONDS);
      latencyTimer.stop();
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    metrics.pushConnection.inc();
  }

  @Override
  public void channelInactive(TransportClient client) {
    metrics.pushConnection.dec();
  }

  public static class WrapPushMetricsCallBack implements RpcResponseCallback {
    private RpcResponseCallback callback;
    private PushMetrics metrics;

    public WrapPushMetricsCallBack(PushMetrics metrics, RpcResponseCallback callback) {
      this.metrics = metrics;
      this.callback = callback;
    }

    public Meter getDataThroughputMetrics() {
      return metrics.pushDataThroughput;
    }

    @Override
    public void onSuccess(ByteBuffer response) {
      // Copy response if response ByteBuffer is not Empty. See. onSuccess Doc
      if (response.remaining() > 0) {
        ByteBuffer wrappedResponse = ByteBuffer.allocate(response.remaining());
        wrappedResponse.put(response);
        wrappedResponse.flip();
        callback.onSuccess(wrappedResponse);
      } else {
        callback.onSuccess(response);
      }
    }

    @Override
    public void onFailure(Throwable e) {
      metrics.pushDataFailed.mark();
      callback.onFailure(e);
    }
  }

  /**
   * A simple class to wrap all push service wrapper metrics
   */
  private class PushMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    private final Meter pushDataQPS = new Meter();
    private final Timer pushDataLatency = new Timer();
    private final Timer pushDataE2ELatency = new Timer();
    private final Meter pushDataThroughput = new Meter();
    private final Meter pushDataFailed = new Meter();

    private final Counter pushConnection = new Counter();

    private PushMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put(WorkerSource.workerSource().buildEventQPSName("PushData"), pushDataQPS);
      allMetrics.put(WorkerSource.workerSource().buildEventLatencyName("PushData"), pushDataLatency);
      allMetrics.put(WorkerSource.workerSource().buildEventLatencyName("PushDataE2E"), pushDataE2ELatency);
      allMetrics.put(WorkerSource.workerSource().buildDataThroughputName("PushData"), pushDataThroughput);
      allMetrics.put(WorkerSource.workerSource().buildEventFailedQPSName("PushData"), pushDataFailed);
      allMetrics.put(WorkerSource.workerSource().buildConnectionName("push"), pushConnection);
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
