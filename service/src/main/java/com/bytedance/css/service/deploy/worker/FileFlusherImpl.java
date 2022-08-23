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

import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileFlusherImpl extends FileFlusher {

  private static final Logger logger = LoggerFactory.getLogger(FileFlusherImpl.class);

  private final String flushId;
  // Flush means disk file flusher
  // HdfsFlush means hdfs file flusher
  // See. FileFlusher definition
  private final String flusherType;
  private int queueCapacity;
  private int bufferSize;

  private LinkedBlockingQueue<FlushTask> flushTaskQueue = null;

  private final Thread flushThread;

  public FileFlusherImpl(
      String flushId,
      String flusherType,
      int queueCapacity,
      int bufferSize) throws Exception {
    this.flushId = flushId;
    this.flusherType = flusherType;
    this.queueCapacity = queueCapacity;
    this.bufferSize = bufferSize;
    flushTaskQueue = new LinkedBlockingQueue<>(this.queueCapacity);

    FileWriterMetrics.instance().registerFlushTaskQueueSize(() -> flushTaskQueue.size());
    Meter fileFlushThroughput = FileWriterMetrics.instance().getThroughput(this.flusherType);
    Meter fileFlushQPS = FileWriterMetrics.instance().getQPS(this.flusherType);

    flushThread = new Thread(String.format("DiskFileFlusher-%s", flushId)) {
      @Override
      public void run() {
        while (true) {
          try {
            FlushTask task = flushTaskQueue.take();

            long dataSize = task.dataSize();
            // flush buffer into disk file
            task.flush();
            fileFlushQPS.mark();
            fileFlushThroughput.mark(dataSize);

            task.getNotifier().getNumPendingFlushes().decrementAndGet();
          } catch (Exception ex) {
            logger.error("DiskFileFlusher flushTaskQueue abnormal exception.", ex);
            throw new RuntimeException(ex);
          }
        }
      }
    };

    flushThread.setDaemon(true);
    flushThread.setUncaughtExceptionHandler((t, e) -> {
      logger.error(String.format("DiskFileFlusher flush thread %s abort abnormally.", t.getName()), e);
      System.exit(-1);
    });
    flushThread.start();
  }

  @Override
  public boolean submitTask(FlushTask task, long timeoutMs) {
    try {
      return flushTaskQueue.offer(task, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int pendingFlushTaskNum() {
    return flushTaskQueue.size();
  }
}
