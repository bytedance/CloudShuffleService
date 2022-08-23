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

package com.bytedance.css.service.deploy.worker;

import com.codahale.metrics.*;

import java.util.HashMap;
import java.util.Map;

public class FileWriterMetrics implements MetricSet {

  private static final FileWriterMetrics fileWriterMetrics = new FileWriterMetrics();

  public static FileWriterMetrics instance() {
    return fileWriterMetrics;
  }

  private final Map<String, Metric> allMetrics;

  // disk type metrics
  public final Meter diskFlushQPS = new Meter();
  public final Timer diskFlushLatency = new Timer();
  public final Meter diskFlushThroughput = new Meter();
  public final Meter diskRealFlushThroughput = new Meter();
  public final Meter diskFlushFailed = new Meter();
  public final Counter diskOpenedFileNum = new Counter();

  // hdfs type metrics
  public final Meter hdfsFlushQPS = new Meter();
  public final Timer hdfsFlushLatency = new Timer();
  public final Meter hdfsFlushThroughput = new Meter();
  public final Meter hdfsFlushFailed = new Meter();
  public final Counter hdfsOpenedFileNum = new Counter();

  private FileWriterMetrics() {
    allMetrics = new HashMap<>();
    WorkerSource workerSource = WorkerSource.workerSource();
    allMetrics.put(workerSource.buildEventQPSName(FileFlusher.DISK_FLUSHER_TYPE), diskFlushQPS);
    allMetrics.put(workerSource.buildEventLatencyName(FileFlusher.DISK_FLUSHER_TYPE), diskFlushLatency);
    allMetrics.put(workerSource.buildDataThroughputName(FileFlusher.DISK_FLUSHER_TYPE), diskFlushThroughput);
    allMetrics.put(workerSource.buildDataThroughputName(FileFlusher.DISK_REAL_FLUSHER_TYPE), diskRealFlushThroughput);
    allMetrics.put(workerSource.buildEventFailedQPSName(FileFlusher.DISK_REAL_FLUSHER_TYPE), diskFlushFailed);
    allMetrics.put(workerSource.buildOpenedFileName(""), diskOpenedFileNum);

    allMetrics.put(workerSource.buildEventQPSName(FileFlusher.HDFS_FLUSHER_TYPE), hdfsFlushQPS);
    allMetrics.put(workerSource.buildEventLatencyName(FileFlusher.HDFS_FLUSHER_TYPE), hdfsFlushLatency);
    allMetrics.put(workerSource.buildDataThroughputName(FileFlusher.HDFS_FLUSHER_TYPE), hdfsFlushThroughput);
    allMetrics.put(workerSource.buildEventFailedQPSName(FileFlusher.HDFS_FLUSHER_TYPE), hdfsFlushFailed);
    allMetrics.put(workerSource.buildOpenedFileName("hdfs"), hdfsOpenedFileNum);
  }

  public Meter getThroughput(String flusherType) {
    if (flusherType.equals(FileFlusher.HDFS_FLUSHER_TYPE)) {
      return hdfsFlushThroughput;
    } else {
      return diskFlushThroughput;
    }
  }

  public Meter getQPS(String flusherType) {
    if (flusherType.equals(FileFlusher.HDFS_FLUSHER_TYPE)) {
      return hdfsFlushQPS;
    } else {
      return diskFlushQPS;
    }
  }

  public void registerFlushTaskQueueSize(Gauge<Integer> gauge) {
    WorkerSource workerSource = WorkerSource.workerSource();
    allMetrics.put(workerSource.buildFlushQueueName(), gauge);
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return allMetrics;
  }
}
