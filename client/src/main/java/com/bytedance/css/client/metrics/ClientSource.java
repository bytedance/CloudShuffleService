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

package com.bytedance.css.client.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSource extends BaseSource {

  private static final Logger logger = LoggerFactory.getLogger(ClientSource.class);

  private static volatile ClientSource clientSource;

  public static ClientSource instance() {
    return instance("dummy", "test");
  }

  public static ClientSource instance(String namespace, String application) {
    if (clientSource == null) {
      synchronized (ClientSource.class) {
        if (clientSource == null) {
          clientSource = new ClientSource(namespace, application);
          logger.info(String.format("%s use cssMetricsPrefix %s",
            clientSource.getClass().getName(), clientSource.cssMetricsPrefix()));
        }
      }
    }
    return clientSource;
  }

  private ClientSource(String namespace, String application) {
    super(namespace, application);
    initMetricSet();
  }

  public Meter batchPushThroughput;
  public Timer mapperEndWaitTimer;
  public Timer reducerFileGroupsTimer;
  public Counter dataLostCounter;
  public Timer pushDataWaitTimer;
  public Histogram pushDataRawSizeHistogram;
  public Histogram pushDataSizeHistogram;

  private void initMetricSet() {
    batchPushThroughput = registry.meter(
      String.format("%s#css.v2.client.batchPush.throughput", cssMetricsPrefix()));
    mapperEndWaitTimer = registry.timer(
      String.format("%s#css.v2.client.mapper.end.wait.time", cssMetricsPrefix()));
    reducerFileGroupsTimer = registry.timer(
      String.format("%s#css.v2.client.reducerFileGroups.get.time", cssMetricsPrefix()));
    dataLostCounter = registry.counter(
      String.format("%s#css.v2.client.dataLost.count", cssMetricsPrefix()));
    pushDataWaitTimer = registry.timer(
      String.format("%s#css.v2.client.pushData.wait.time", cssMetricsPrefix()));
    pushDataRawSizeHistogram = registry.histogram(
      String.format("%s#css.v2.client.pushData.rawSize", cssMetricsPrefix()));
    pushDataSizeHistogram = registry.histogram(
      String.format("%s#css.v2.client.pushData.size", cssMetricsPrefix()));
  }

  @Override
  public String sourceName() {
    return "";
  }
}
