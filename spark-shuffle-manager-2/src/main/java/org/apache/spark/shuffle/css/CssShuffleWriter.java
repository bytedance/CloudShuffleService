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
import com.bytedance.css.common.CssConf;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.css.sort.CssShuffleExternalSorter;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerId$;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {
  // Refer to UnsafeShuffleWriter
  private static final Logger logger = LoggerFactory.getLogger(CssShuffleWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;
  private final int PARTITION_GROUP_PUSH_BUFFER_SIZE;
  private final int PUSH_QUEUE_CAPACITY;

  private final TaskMemoryManager memoryManager;
  private final ShuffleDependency<K, V, C> dep;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final ShuffleWriteMetricsAdapter writeMetricsAdapter;

  // shuffle meta info
  private final String appId;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final CssConf cssConf;
  private final SparkConf sparkConf;
  private final ShuffleClient cssShuffleClient;
  private final int numMappers;
  private final int numPartitions;

  @Nullable private CssShuffleExternalSorter sorter;
  @Nullable private MapStatus mapStatus;
  // for overall mapStatus metrics update with each partitions
  private final long[] partitionSizes;

  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }

  private final MyByteArrayOutputStream serBuffer;
  private final SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private volatile boolean stopping = false;
  private AsyncPushDataTaskManager taskManager;

  private final int initialSortBufferSize;
  private final long sortPushSpillSizeThreshold;
  private final long sortPushSpillRecordThreshold;


  public CssShuffleWriter(
      BaseShuffleHandle<K, V, C> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      CssConf cssConf,
      ShuffleClient cssShuffleClient) throws IOException {
    dep = handle.dependency();
    serializer = dep.serializer().newInstance();
    partitioner = dep.partitioner();
    this.taskContext = taskContext;
    this.memoryManager = taskContext.taskMemoryManager();
    writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.writeMetricsAdapter = new ShuffleWriteMetricsAdapter() {
      @Override
      public void incBytesWritten(long v) {
        writeMetrics.incBytesWritten(v);
      }

      @Override
      public void incWriteTime(long v) {
        writeMetrics.incWriteTime(v);
      }

      @Override
      public void incRecordsWritten(long v) {
        writeMetrics.incRecordsWritten(v);
      }
    };
    appId = ((CssShuffleHandle) handle).appId();
    shuffleId = dep.shuffleId();
    this.mapId = mapId;
    this.sparkConf = sparkConf;
    this.cssConf = cssConf;
    this.cssShuffleClient = cssShuffleClient;
    numMappers = handle.numMaps();
    numPartitions = handle.dependency().partitioner().numPartitions();
    this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize", 4096);

    // CSS Configuration
    PARTITION_GROUP_PUSH_BUFFER_SIZE = (int) CssConf.partitionGroupPushBufferSize(cssConf);
    PUSH_QUEUE_CAPACITY = CssConf.pushQueueCapacity(cssConf);
    sortPushSpillSizeThreshold = CssConf.sortPushSpillSizeThreshold(cssConf);
    sortPushSpillRecordThreshold = CssConf.sortPushSpillRecordThreshold(cssConf);

    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);

    // written bytes and record metrics report
    partitionSizes = new long[numPartitions];
  }

  private void createExtSorter() {
    assert (sorter == null);
    sorter = new CssShuffleExternalSorter(
      memoryManager,
      taskContext,
      taskManager,
      sortPushSpillSizeThreshold,
      sortPushSpillRecordThreshold,
      initialSortBufferSize,
      PARTITION_GROUP_PUSH_BUFFER_SIZE,
      ShuffleClient.shufflePartitionGroupMap.get(shuffleId),
      sparkConf,
      writeMetricsAdapter);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    if (records.hasNext()) {
      taskManager = new AsyncPushDataTaskManager(appId, shuffleId, mapId, taskContext.attemptNumber(),
        numMappers, numPartitions, cssShuffleClient, writeMetricsAdapter, PUSH_QUEUE_CAPACITY);

      createExtSorter();

      if (dep.mapSideCombine()) {
        if (dep.aggregator().isEmpty()) {
          throw new UnsupportedOperationException("map side combine");
        }
        doSortWrite(dep.aggregator().get().combineValuesByKey(records, taskContext));
      } else {
        doSortWrite(records);
      }
    }
    close();
  }

  private void doSortWrite(scala.collection.Iterator iterator) throws IOException {
    logger.info("Enter doSortWrite");
    final scala.collection.Iterator<Product2<K, ?>> records = iterator;
    boolean success = false;
    try {
      while (records.hasNext()) {
        assert(sorter != null);
        final Product2<K, ?> record = records.next();
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);
        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();

        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);

        sorter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
      }
      sorter.close();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during cleanup.", e);
          }
        }
      }
    }
  }

  // send out last batches of data and call MapperEnd
  // also update MapStatus
  private void close() throws IOException {

    // wait for taskManager until noMorePendingTask
    if (taskManager != null) {
      taskManager.awaitTermination();
      // add flush buffer writer size & direct writer size
      long[] flushBufferPartitionSizes = taskManager.getPartitionSizes();
      for (int i = 0; i < numPartitions; i++) {
        partitionSizes[i] += flushBufferPartitionSizes[i];
      }
    }

    // Send MapperEnd will trigger ShuffleClient limitMaxInFlight
    // which requires all pushDataRequest being finished.
    long waitStartTime = System.nanoTime();
    cssShuffleClient.mapperEnd(appId, shuffleId, mapId, taskContext.attemptNumber(), numMappers);
    writeMetrics.incWriteTime(System.nanoTime() - waitStartTime);

    BlockManagerId dummyId = BlockManagerId$.MODULE$.apply(
      "CSS-Writer", "127.0.0.1", 9527, Option.apply(null));
    mapStatus = MapStatus$.MODULE$.apply(dummyId, partitionSizes);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes);

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (taskManager != null) {
          taskManager.setStopping();
        }
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (taskManager != null) {
        taskManager = null;
      }
      if (cssShuffleClient != null) {
        cssShuffleClient.mapperClose(appId, shuffleId, mapId, taskContext.attemptNumber());
      }
    }
  }
}
