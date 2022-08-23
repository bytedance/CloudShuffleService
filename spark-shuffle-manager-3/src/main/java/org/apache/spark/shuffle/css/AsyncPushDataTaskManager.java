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

package org.apache.spark.shuffle.css;

import com.bytedance.css.client.ShuffleClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncPushDataTaskManager {

  private final long WAIT_TIME_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

  private final String appId;
  private final int shuffleId;
  private final int mapperId;
  private final int mapperAttemptId;
  private final int numMappers;
  private final int numPartitions;
  private final ShuffleClient shuffleClient;
  private final ShuffleWriteMetricsAdapter writeMetrics;
  private final int pushQueueCapacity;

  // TODO remove it
  private class PushDataTask {
    int partitionId;
    final byte[] buffer = new byte[0];
    int size;
  }

  private final LinkedBlockingQueue<PushDataTask> pendingQueue;
  private final LinkedBlockingQueue<PushDataTask> pushingQueue;

  // remain flush buffer writer size with each partitions.
  private final long[] partitionSizes;

  // Refer to Spark ContextWaiter
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition noMorePendingTask = lock.newCondition();

  // whenever there is exception occur, we should stop the entire mapperAttempt
  private final AtomicReference<IOException> exception = new AtomicReference<>();

  private volatile boolean terminated;
  private volatile boolean stopping;

  private final Thread pushDataTaskProcessor;

  public AsyncPushDataTaskManager(
      String appId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int numMappers,
      int numPartitions,
      ShuffleClient shuffleClient,
      ShuffleWriteMetricsAdapter writeMetrics,
      int pushQueueCapacity) throws IOException {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapperId = mapperId;
    this.mapperAttemptId = mapperAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.shuffleClient = shuffleClient;
    this.writeMetrics = writeMetrics;
    this.pushQueueCapacity = pushQueueCapacity;

    // written bytes and record metrics report
    partitionSizes = new long[numPartitions];

    pendingQueue = new LinkedBlockingQueue<>(this.pushQueueCapacity);
    pushingQueue = new LinkedBlockingQueue<>(this.pushQueueCapacity);

    // initialize of pending queue, put PushDataTasks that has already with initialized byte buffer.
    for (int i = 0; i < pushQueueCapacity; i++) {
      try {
        pendingQueue.put(new PushDataTask());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    // create processor thread and start
    pushDataTaskProcessor = new Thread(
      String.format("AsyncPushDataTaskManager for %s-%s-%s-%s", appId, shuffleId, mapperId, mapperAttemptId)) {
      @Override
      public void run() {
        while (!terminated && !stopping && exception.get() == null) {
          try {
            PushDataTask task = pushingQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
            if (task == null) {
              continue;
            }
            executePushData(task.partitionId, task.buffer, task.size);
            returnTaskBuffer(task);
          } catch (InterruptedException e) {
            exception.set(new IOException(e));
          } catch (IOException e) {
            exception.set(e);
          }
        }
      }
    };
    pushDataTaskProcessor.start();
  }

  public void setStopping() {
    stopping = true;
  }

  private void returnTaskBuffer(PushDataTask task) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      pendingQueue.put(task);
      if (pendingQueue.remainingCapacity() == 0) {
        noMorePendingTask.signal();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exception.set(new IOException(e));
    } finally {
      lock.unlock();
    }
  }

  public int directBatchPush(
      int[] reducerIdArray,
      byte[] data,
      int[] offsetArray,
      int[] lengthArray) throws IOException {
    long pushStartTime = System.nanoTime();
    int[] partitionWrittenBytes = shuffleClient.batchPushData(
      appId,
      shuffleId,
      mapperId,
      mapperAttemptId,
      reducerIdArray,
      data,
      offsetArray,
      lengthArray,
      numMappers,
      numPartitions,
      false);
    for (int i = 0; i < reducerIdArray.length; i ++) {
      partitionSizes[reducerIdArray[i]] += partitionWrittenBytes[i];
    }
    int bytesWritten = Arrays.stream(partitionWrittenBytes).sum();
    writeMetrics.incBytesWritten(bytesWritten);
    writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
    return bytesWritten;
  }

  public void safeDirectBatchPush(int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray) {
    try {
      this.directBatchPush(reducerIdArray, data, offsetArray, lengthArray);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void safeAddPushDataTask(int partitionId, byte[] buffer, int size) {
    try {
      this.addPushDataTask(partitionId, buffer, size);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void addPushDataTask(int partitionId, byte[] buffer, int size) throws IOException {
    try {
      // fetch task buffer from pendingQueue
      // copy buffer and set parameters
      // put this task into pushingQueue
      // wait for the local thread to fetch from pushingQueue and do executePushData
      PushDataTask task = null;
      while (task == null) {
        checkException();
        task = pendingQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
      task.partitionId = partitionId;
      System.arraycopy(buffer, 0, task.buffer, 0, size);
      task.size = size;
      while (!pushingQueue.offer(task, WAIT_TIME_NANOS, TimeUnit.NANOSECONDS)) {
        checkException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
  }

  private void executePushData(int partitionId, byte[] buffer, int size) throws IOException {
    long pushStartTime = System.nanoTime();
    int bytesWritten = 0;
    partitionSizes[partitionId] += bytesWritten;
    writeMetrics.incBytesWritten(bytesWritten);
    writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
  }

  public long[] getPartitionSizes() {
    return partitionSizes;
  }

  private void checkException() throws IOException {
    if (exception.get() != null) {
      throw exception.get();
    }
  }

  void awaitTermination() throws IOException {
    try {
      lock.lockInterruptibly();
      // when all task finished, pendingQueue.remainingCapacity = 0 and break
      while (pendingQueue.remainingCapacity() > 0 && exception.get() == null) {
        noMorePendingTask.await(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exception.set(new IOException(e));
    } finally {
      lock.unlock();
    }

    terminated = true;
    pendingQueue.clear();
    pushingQueue.clear();
    checkException();
  }
}
