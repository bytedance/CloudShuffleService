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

package com.bytedance.css.client.impl;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.client.MapperAttemptStatus;
import com.bytedance.css.client.compress.Compressor;
import com.bytedance.css.client.compress.CssCompressorFactory;
import com.bytedance.css.client.metrics.ClientSource;
import com.bytedance.css.client.stream.CssInputStream;
import com.bytedance.css.client.stream.disk.CssRemoteDiskEpochReader;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.exception.PartitionInfoNotFoundException;
import com.bytedance.css.common.protocol.*;
import com.bytedance.css.common.rpc.RpcAddress;
import com.bytedance.css.common.rpc.RpcEndpointRef;
import com.bytedance.css.common.rpc.RpcEnv;
import com.bytedance.css.common.unsafe.Platform;
import com.bytedance.css.common.util.ThreadUtils;
import com.bytedance.css.common.util.Utils;
import com.bytedance.css.network.TransportContext;
import com.bytedance.css.network.buffer.NettyManagedBuffer;
import com.bytedance.css.network.client.RpcResponseCallback;
import com.bytedance.css.network.client.TransportClient;
import com.bytedance.css.network.client.TransportClientFactory;
import com.bytedance.css.network.protocol.BatchPushDataRequest;
import com.bytedance.css.network.server.NoOpRpcHandler;
import com.bytedance.css.network.util.TransportConf;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ShuffleClientImpl extends ShuffleClient {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleClient.class);

  // Configuration
  private final CssConf cssConf;
  private final ShuffleMode defaultShuffleMode;
  private final long rotateThreshold;
  private final int pushIoMaxRetries;
  private final long pushIoRetryWaitMs;
  private final int partitionGroupPushRetries;

  // For RPC
  private RpcEnv rpcEnv;
  private RpcEndpointRef masterRpcRef;
  private TransportClientFactory clientFactory;

  // shuffleId in registering
  // TODO shuffle register control when push data for diff engine, like mr flink.
  private Set<Integer> registering = new ConcurrentSet<>();

  // multi control for reallocate
  private Set<String> reallocating = new ConcurrentSet<>();
  private ReentrantLock reallocatingLock = new ReentrantLock();
  private Condition pushDataAwaitCondition = reallocatingLock.newCondition();
  private Condition reallocatingAwaitCondition = reallocatingLock.newCondition();

  // multi control for read partition info
  private Set<Integer> reading = new ConcurrentSet<>();

  // key: Utils.makeMapperKey(shuffleId-mapId-attemptId)
  // value: MapperAttemptStatus
  private Map<String, MapperAttemptStatus> statusMap = new ConcurrentHashMap<>();

  // key: shuffleId
  // value: Set[Utils.makeMapperKey(shuffleId-mapId-attemptId)]
  private final ConcurrentHashMap<Integer, ConcurrentSet<String>> mapperEndMap = new ConcurrentHashMap<>();

  // key: shuffleId
  // value: (reduceId, Array[CommittedPartitionInfo])
  private Map<Integer, ConcurrentHashMap<Integer, CommittedPartitionInfo[]>> reducerFileGroups =
    new ConcurrentHashMap<>();

  // key: shuffleId
  // value: mapperAttemptIds remain all success map attempts to do
  private Map<Integer, int[]> mapperAttempts = new ConcurrentHashMap<>();
  private Map<Integer, HashSet<FailedPartitionInfoBatch>> batchBlacklistMap = new ConcurrentHashMap<>();

  // Push Data Retry ThreadPool
  private ThreadPoolExecutor pushDataRetryThreadPool;

  // app heart beat
  private Thread appHeartBeatThread;
  private volatile boolean appHeartBeatStarted = false;
  private Object appHeartBeatLock = new Object();

  // metrics for push data
  private Timer mapperEndWaitTimer = ClientSource.instance().mapperEndWaitTimer;
  private Timer pushDataWaitTimer = ClientSource.instance().pushDataWaitTimer;
  private Timer reducerFileGroupsTimer = ClientSource.instance().reducerFileGroupsTimer;
  private Counter dataLostCounter = ClientSource.instance().dataLostCounter;
  private Histogram pushDataRawSize = ClientSource.instance().pushDataRawSizeHistogram;
  private Histogram pushDataSize = ClientSource.instance().pushDataSizeHistogram;
  private Meter batchPushThroughput = ClientSource.instance().batchPushThroughput;

  // Compressor
  private CssCompressorFactory compressorFactory;
  private ThreadLocal<Compressor> compressorThreadLocal;

  // backpressure send rate control
  // Global dynamics backpressure limit.
  private final boolean backpressureEnable;
  private final boolean backpressureLogEnable;
  private final int backpressureMaxConcurrency;
  private final int backpressureMinLimit;
  private final double backpressureSmoothing;
  private final int backpressureLongWindow;
  private final double backpressureRttTolerance;
  private final int backpressureQueueSize;
  // fix rate backpressure limit.
  private final int fixRateLimit;

  private final ConcurrentHashMap<String, BlockingLimiter<Void>> limiters =
      new ConcurrentHashMap<>();
  private final int reallocateFailedMaxTimes;

  private final boolean failedBatchBlacklistEnable;

  public ShuffleClientImpl(CssConf cssConf) {
    this.cssConf = cssConf;
    this.failedBatchBlacklistEnable = CssConf.failedBatchBlacklistEnable(cssConf);
    this.defaultShuffleMode = CssConf.shuffleMode(cssConf);
    this.rotateThreshold = CssConf.epochRotateThreshold(cssConf);
    this.reallocateFailedMaxTimes = CssConf.clientReallocateFailedMaxTimes(cssConf);
    this.pushDataRetryThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "PushDataRequest Retry ThreadPool", CssConf.pushDataRetryThreads(cssConf), 60);
    this.compressorFactory = new CssCompressorFactory(this.cssConf);
    this.compressorThreadLocal = ThreadLocal.withInitial(() -> this.compressorFactory.getCompressor());
    this.pushIoMaxRetries = CssConf.pushIoMaxRetries(cssConf);
    this.pushIoRetryWaitMs = CssConf.pushIoRetryWaitMs(cssConf);
    this.partitionGroupPushRetries = CssConf.partitionGroupPushRetries(cssConf);

    // backpressure init
    this.backpressureEnable = CssConf.backpressureEnable(this.cssConf);
    this.backpressureLogEnable = CssConf.backpressureLogEnable(this.cssConf);
    this.backpressureMaxConcurrency = CssConf.backpressureMaxConcurrency(this.cssConf);
    this.backpressureMinLimit = CssConf.backpressureMinLimit(this.cssConf);
    this.backpressureSmoothing = CssConf.backpressureSmoothing(this.cssConf);
    this.backpressureLongWindow = CssConf.backpressureLongWindow(this.cssConf);
    this.backpressureRttTolerance = CssConf.backpressureRttTolerance(this.cssConf);
    this.backpressureQueueSize = CssConf.backpressureQueueSize(this.cssConf);

    this.fixRateLimit = CssConf.fixRateLimitThreshold(this.cssConf);

    rpcEnvInit();
  }

  private BlockingLimiter getBackpressureLimiter(String hostPortStr) {
    BlockingLimiter limit;
    if (!backpressureEnable) {
      limit = limiters.computeIfAbsent("fixRateLimiter", (s) ->
        BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(
          FixedLimit.of(this.fixRateLimit)
        ).build()));
    } else {
      limit = limiters.computeIfAbsent(hostPortStr, (s) -> {
        Gradient2Limit gradient2Limit = Gradient2Limit.newBuilder()
          .minLimit(backpressureMinLimit)
          .smoothing(backpressureSmoothing)
          .longWindow(backpressureLongWindow)
          .rttTolerance(backpressureRttTolerance)
          .queueSize(backpressureQueueSize)
          .maxConcurrency(backpressureMaxConcurrency).build();
        if (backpressureLogEnable) {
          gradient2Limit.notifyOnChange((newLimit) -> {
            logger.info("Update to new limit {}", newLimit);
          });
        }
        return BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(
          gradient2Limit
        ).build());
      });
    }
    return limit;
  }

  private void rpcEnvInit() {
    TransportConf transportConf =
      Utils.fromCssConf(cssConf, TransportModuleConstants.DATA_MODULE, CssConf.dataThreads(cssConf));
    TransportContext transportContext = new TransportContext(transportConf, new NoOpRpcHandler(), false);
    this.clientFactory = transportContext.createClientFactory(Lists.newArrayList());

    rpcEnv = RpcEnv.create(RpcNameConstants.SHUFFLE_CLIENT_SYS, Utils.localHostName(), 0, cssConf, true);
    masterRpcRef = rpcEnv.setupEndpointRef(
      RpcAddress.fromCssURL(CssConf.masterAddress(cssConf)), RpcNameConstants.MASTER_EP);
  }

  private void checkBatchData(int[] reducerIdArray, int[] offsetArray, int[] lengthArray) throws IOException {
    if (reducerIdArray.length != offsetArray.length || reducerIdArray.length != lengthArray.length) {
      throw new IOException("Tuple3 reducerIdArray offsetArray lengthArray not eq!");
    }

    for (int i = 0; i < offsetArray.length - 1; i ++) {
      if (offsetArray[i] + lengthArray[i] != offsetArray[i + 1]) {
        throw new IOException("offsetArray lengthArray metadata incorrect!");
      }
    }
  }

  @Override
  public int[] batchPushData(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int[] reducerIdArray,
      byte[] data,
      int[] offsetArray,
      int[] lengthArray,
      int numMappers,
      int numPartitions,
      boolean skipCompress) throws IOException {

    checkBatchData(reducerIdArray, offsetArray, lengthArray);

    // TODO When other engines like mr or flink cannot broadcast the register shuffle result,
    // we can initialize register it first. when sending data

    String shuffleKey = Utils.getShuffleKey(applicationId, shuffleId);
    PartitionGroupManager manager = shufflePartitionGroupMap.get(shuffleId);

    // make sure all reducerId belong to single partitionGroup
    long distinctGroups = Arrays.stream(reducerIdArray).map(manager::groupId).distinct().count();
    if (distinctGroups != 1) {
      throw new IOException("reducerIdArray not belong to single partitionGroup");
    }

    int partitionGroupId = manager.groupId(reducerIdArray[0]);
    waitUntilReallocatePartitionGroupEnded(applicationId, shuffleId, partitionGroupId);

    PartitionGroup partitionGroup = shufflePartitionGroupMap.get(shuffleId).getGroup(partitionGroupId);

    String mapperKey = Utils.getMapperKey(shuffleId, mapperId, mapperAttemptId);
    MapperAttemptStatus status = statusMap.computeIfAbsent(mapperKey, (s) -> new MapperAttemptStatus());

    if (status.getException() != null) {
      throw status.getException();
    }

    // before sending data, record the current send request + 1
    final int reqBatchId = status.getNextBatchId();
    final int[] partitionBatchIds =
      Arrays.stream(reducerIdArray).map(reducerId -> status.getNextBatchId()).toArray();
    status.InFlightReqs.incrementAndGet();

    int writeBytes = 0;
    int[] partitionWrittenBytes = new int[reducerIdArray.length];
    List<Integer> offsetLists = new ArrayList<>();
    offsetLists.add(0); // init offset start with 0. it will be used for worker bytebuf.slice(index, length)
    // use current batchId to mark all reduce data.
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer(reducerIdArray.length);
    for (int i = 0; i < reducerIdArray.length; i ++) {
      int partitionBatchId = partitionBatchIds[i];
      byte[] batchBytes;
      if (skipCompress) {
        batchBytes = addHeader(mapperId, mapperAttemptId, partitionBatchId, data, offsetArray[i], lengthArray[i]);
      } else {
        batchBytes = addHeaderAndCompress(
          mapperId,
          mapperAttemptId,
          partitionBatchId,
          data,
          offsetArray[i],
          lengthArray[i]);
      }
      compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(batchBytes));
      offsetLists.add(offsetLists.get(offsetLists.size() - 1) + batchBytes.length);
      partitionWrittenBytes[i] = batchBytes.length;
      writeBytes += batchBytes.length;
    }
    int[] offsets = offsetLists.stream().mapToInt(Integer::valueOf).toArray();

    // TODO use compositeByteBuf directly to send to transportClient
    byte[] sendBytes = new byte[writeBytes];
    compositeByteBuf.readBytes(sendBytes);

    int originTotalLength = Arrays.stream(lengthArray).sum();
    int originTotalWriteBytes = writeBytes;

    asyncBatchPushDataWithRetry(partitionGroup, applicationId, shuffleId, mapperId, mapperAttemptId,
      reqBatchId, partitionBatchIds, reducerIdArray, sendBytes, offsets, status, 0,
      originTotalLength, originTotalWriteBytes);

    return partitionWrittenBytes;
  }

  private void asyncBatchPushDataWithRetry(
      PartitionGroup partitionGroup,
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int reqBatchId,
      int[] partitionBatchIds,
      int[] reducerIdArray,
      byte[] compressData,
      int[] offsets,
      MapperAttemptStatus status,
      final int retryCount,
      int originTotalLength,
      int originTotalWriteBytes) throws IOException {

    String shuffleKey = Utils.getShuffleKey(applicationId, shuffleId);
    String mapperKey = Utils.getMapperKey(shuffleId, mapperId, mapperAttemptId);

    if (status.getException() != null) {
      throw status.getException();
    }

    int pgId = partitionGroup.partitionGroupId;
    int epochId = partitionGroup.epochId;
    String pairAddressStr = partitionGroup.makeReplicaAddressStr();

    String shuffleMode = defaultShuffleMode.toString();
    String tmpRotateThreshold = String.valueOf(rotateThreshold);

    Optional<Limiter.Listener> listener = Optional.empty();
    Timer.Context waitTimer = pushDataWaitTimer.time();
    listener = getBackpressureLimiter(pairAddressStr).acquire(null);
    waitTimer.stop();

    Optional<Limiter.Listener> finalListener = listener;
    if (!finalListener.isPresent()) {
      // listener not defined means after certain timeout, still no permit granted from limiter
      throw new IOException("Backpressure limit timeout!");
    }

    if (retryCount > this.partitionGroupPushRetries) {
      throw new IOException("reallocatePartitionGroup retry still failed!");
    }

    List<WorkerAddress> originPair = partitionGroup.getReplicaWorkers();
    List<WorkerAddress> pair = new ArrayList<>(partitionGroup.getReplicaWorkers());
    // shuffle this worker pair to random send order.
    Collections.shuffle(pair);
    final int expectRet = pair.size();

    List<CompletableFuture<Integer>> pairFutures = pair.stream().map(wa -> {
      // return 0 means push successful
      // return 1 means rotate force write successful
      // return > expectRet means current push request failed
      CompletableFuture<Integer> result = new CompletableFuture<>();

      RpcResponseCallback callback = new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          int remaining = response.remaining();
          if (remaining == 1 && response.duplicate().get() == CssStatusCode.EpochShouldRotate.getValue()) {
            logger.warn(String.format("batchPushDataRequest to %s:%s Hit EpochShouldRotate " +
              "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s epochId %s batchId %s",
              wa.host, wa.port,
              applicationId, shuffleId, mapperId, mapperAttemptId,
              pgId, epochId, reqBatchId));
            result.complete(1);
          } else if (remaining == 1 && response.duplicate().get() == CssStatusCode.StageEnded.getValue()) {
            logger.warn(String.format("batchPushDataRequest to %s:%s Hit StageEnded " +
              "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s epochId %s batchId %s",
              wa.host, wa.port,
              applicationId, shuffleId, mapperId, mapperAttemptId,
              pgId, epochId, reqBatchId));
            mapperEndMap.computeIfAbsent(shuffleId, (id) -> new ConcurrentSet<>()).add(mapperKey);
            result.complete(expectRet + 1);
          } else {
            result.complete(0);
          }
        }

        @Override
        public void onFailure(Throwable e) {
          // break from worker callback, since next pushData will check exception and fail in the first place
          // check whether failed with PartitionInfoNotFoundException.
          // it means worker may happen an internal server error.
          if (e != null && e instanceof PartitionInfoNotFoundException) {
            logger.warn(String.format("batchPushDataRequest to %s:%s Hit PartitionInfoNotFoundException " +
              "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s epochId %s batchId %s",
              wa.host, wa.port,
              applicationId, shuffleId, mapperId, mapperAttemptId,
              pgId, epochId, reqBatchId));
            status.setException(new IOException(e));
          }
          result.complete(expectRet + 2);
        }
      };

      try {
        TransportClient client = createClientWithRetry(wa.host, wa.port, -1);
        long pushDataStartMs = System.currentTimeMillis();
        client.batchPushData(
          new BatchPushDataRequest(shuffleKey, reducerIdArray, epochId, offsets, mapperId,
            originPair.indexOf(wa), shuffleMode, tmpRotateThreshold, pushDataStartMs,
            new NettyManagedBuffer(Unpooled.wrappedBuffer(compressData))), callback);
      } catch (Exception exception) {
        logger.error(
          String.format("batchPushDataRequest to %s:%s Hit ClientSideException " +
            "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s epochId %s batchId %s",
              wa.host, wa.port, applicationId, shuffleId, mapperId, mapperAttemptId,
              pgId, epochId, reqBatchId), exception);
        result.complete(expectRet + 3);
      }
      return result;
    }).collect(Collectors.toList());

    AtomicInteger resultSum = new AtomicInteger(0);
    CompletableFuture.allOf(pairFutures.toArray(new CompletableFuture[0]))
      .whenCompleteAsync((v, t) -> {
        pairFutures.forEach(future -> resultSum.addAndGet(future.getNow(100)));

        boolean isFinished = true;
        if (resultSum.get() <= expectRet) {
          // let's see here 2 pair workers & expectRet = 2
          // if return 0 0 means worker writes successfully
          // if return 0 1 / 1 0 / 1 1 means the strong write is successful and requires rotate
          // when write success. update limiter & writtenEpochSet
          finalListener.get().onSuccess();
          Arrays.stream(reducerIdArray).forEach(reducerId -> {
            status.writtenEpochSet.add(new PartitionInfo(reducerId, epochId));
          });
          if (resultSum.get() > 0) {
            // after the worker forcibly writes, notify the client to rotate
            reallocatePartitionGroupWithRetry(applicationId, shuffleId, mapperId, mapperAttemptId, partitionGroup);
          }
        } else {
          // means this data write failed
          // update current limiter & failedBatchBlacklist
          finalListener.get().onIgnore();
          if (failedBatchBlacklistEnable) {
            for (int i = 0; i < reducerIdArray.length; i ++) {
              status.failedBatchBlacklist.add(new FailedPartitionInfoBatch(
                reducerIdArray[i], epochId, mapperId, mapperAttemptId, partitionBatchIds[i])
              );
            }
          }
          // reallocate partitionGroup and retry
          if (mapperEnded(shuffleId, mapperId, mapperAttemptId)) {
            // mapper ended skip retry
            logger.warn(String.format("Skip retry since Hit StageEnded " +
              "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s",
                applicationId, shuffleId, mapperId, mapperAttemptId, pgId));
          } else if (retryCount < this.partitionGroupPushRetries) {
            if (!reallocatePartitionGroupWithRetry(applicationId, shuffleId,
              mapperId, mapperAttemptId, partitionGroup)) {
              logger.error(String.format("Encounter %s retry failed for " +
                "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s",
                  retryCount, applicationId, shuffleId, mapperId, mapperAttemptId, pgId));
              status.setException(new IOException("reallocatePartitionGroup failed."));
            } else {
              // double check. since if reallocate find mapperEnd, it can fast skip.
              if (mapperEnded(shuffleId, mapperId, mapperAttemptId)) {
                // mapper ended skip retry
                logger.warn(String.format("Skip retry since Hit StageEnded " +
                    "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s",
                  applicationId, shuffleId, mapperId, mapperAttemptId, pgId));
              } else {
                logger.info(String.format("Enter %s retry for " +
                    "appId %s shuffleId %s mapperId %s mapperAttemptId %s groupId %s",
                  retryCount, applicationId, shuffleId, mapperId, mapperAttemptId, pgId));
                isFinished = false;
                PartitionGroup newPartitionGroup = shufflePartitionGroupMap.get(shuffleId).getGroup(pgId);
                pushDataRetryThreadPool.submit(() -> {
                  try {
                    asyncBatchPushDataWithRetry(newPartitionGroup, applicationId, shuffleId, mapperId, mapperAttemptId,
                      reqBatchId, partitionBatchIds, reducerIdArray, compressData, offsets, status, retryCount + 1,
                      originTotalLength, originTotalWriteBytes);
                  } catch (IOException e) {
                    status.setException(e);
                    status.InFlightReqs.decrementAndGet();
                  } catch (Throwable throwable) {
                    status.setException(new IOException(throwable));
                    status.InFlightReqs.decrementAndGet();
                  }
                });
              }
            }
          } else {
            status.setException(new IOException("reallocatePartitionGroup retry still failed"));
          }
        }
        if (isFinished) {
          pushDataRawSize.update(originTotalLength);
          pushDataSize.update(originTotalWriteBytes);
          batchPushThroughput.mark(originTotalWriteBytes * 2L);
          status.InFlightReqs.decrementAndGet();
        }
      });
  }

  private boolean shouldRetry(Throwable e, int retryCount) {
    boolean isIOException = e instanceof IOException ||
      (e.getCause() != null && e.getCause() instanceof IOException);
    boolean hasRemainingRetries = retryCount < pushIoMaxRetries;
    return isIOException && hasRemainingRetries;
  }

  private TransportClient createClientWithRetry(String remoteHost, int remotePort, int seed) throws Exception {
    TransportClient client = null;
    int retryCount = 0;
    while (true) {
      try {
        client = clientFactory.createClient(remoteHost, remotePort, seed);
        return client;
      } catch (Exception ex) {
        if (shouldRetry(ex, retryCount)) {
          retryCount ++;
          logger.warn("createClient with {}:{} with seed {} failed, retry for the {} time",
            remoteHost, remotePort, seed, retryCount);
          Uninterruptibles.sleepUninterruptibly(pushIoRetryWaitMs, TimeUnit.MILLISECONDS);
        } else {
          logger.error("createClient with {}:{} with seed {} failed, will not retry again",
            remoteHost, remotePort, seed);
          throw ex;
        }
      }
    }
  }

  /**
   * When the reallocate process is encountered during the data transmission process.
   *
   * It is necessary to wait until the new worker node group is reallocated
   * before continuing the process of data writing.
   */
  private void waitUntilReallocatePartitionGroupEnded(
    String applicationId,
    int shuffleId,
    int groupId) throws IOException {
    String groupKey = Utils.getPartitionGroupKey(applicationId, shuffleId, groupId);
    if (reallocating.contains(groupKey)) {
      reallocatingLock.lock();
      // if current partition group is under reallocation, better wait until reallocation done.
      try {
        while (reallocating.contains(groupKey)) {
          try {
            pushDataAwaitCondition.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
        }
      } finally {
        reallocatingLock.unlock();
      }
    }
  }

  /**
   * When push data failed or worker notify should write rotate.
   *
   * Reallocate new worker pairs for the remaining data to be written.
   * multi-threaded reallocate control, block push data requests during reallocate.
   * if a new epoch has been allocated, use the new epoch directly.
   */
  private boolean reallocatePartitionGroupWithRetry(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      PartitionGroup partitionGroup) {
    String groupKey = Utils.getPartitionGroupKey(applicationId, shuffleId, partitionGroup.partitionGroupId);
    String mapperKey = Utils.getMapperKey(shuffleId, mapperId, mapperAttemptId);

    reallocatingLock.lock();
    try {
      while (reallocating.contains(groupKey)) {
        try {
          reallocatingAwaitCondition.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }

      PartitionGroup current = shufflePartitionGroupMap.get(shuffleId).getGroup(partitionGroup.partitionGroupId);
      if (mapperEnded(shuffleId, mapperId, mapperAttemptId)) {
        logger.info(
          String.format("MapperEnded for %s-%s-%s-%s-<PG:%s>-%s during reallocation",
            applicationId, shuffleId, mapperId, mapperAttemptId,
            partitionGroup.partitionGroupId, partitionGroup.epochId));
        return true;
      } else if (current == null) {
        throw new RuntimeException("ReallocatePartitionGroup current NPE.");
      } else if (current.epochId == partitionGroup.epochId) {
        logger.info(
          String.format("Enter allocate state for %s-%s-%s-%s-<PG:%s>-%s",
            applicationId, shuffleId, mapperId, mapperAttemptId,
            partitionGroup.partitionGroupId, partitionGroup.epochId));
        reallocating.add(groupKey);
      } else {
        logger.info(
          String.format("New group already allocated for %s-%s-%s-%s-<PG:%s>-%s using new epoch %s",
            applicationId, shuffleId, mapperId, mapperAttemptId,
            partitionGroup.partitionGroupId, partitionGroup.epochId, current.epochId));
        return true;
      }
    } finally {
      if (!reallocating.contains(groupKey) && !reallocatingLock.hasWaiters(reallocatingAwaitCondition)) {
        pushDataAwaitCondition.signalAll();
      }
      reallocatingLock.unlock();
    }

    int numFailures = 0;
    long waitIntervalMs = CssConf.clientReallocateRetryIntervalMs(cssConf);
    try {
      while (numFailures < reallocateFailedMaxTimes) {
        try {
          logger.info(
            String.format("Executing reallocate operation for %s-%s-%s-%s-<PG:%s>-%s, so far %s times",
              applicationId, shuffleId, mapperId, mapperAttemptId,
              partitionGroup.partitionGroupId, partitionGroup.epochId, numFailures));

          CssRpcMessage.ReallocatePartitionGroupResponse response = masterRpcRef.askSync(new CssRpcMessage
            .ReallocatePartitionGroup(applicationId, shuffleId, mapperId, mapperAttemptId, partitionGroup),
            ClassTag$.MODULE$.apply(CssRpcMessage.ReallocatePartitionGroupResponse.class)
          );

          if (response.statusCode().equals(CssStatusCode.Success)) {
            shufflePartitionGroupMap.get(shuffleId)
              .updateGroup(partitionGroup.partitionGroupId, response.partitionGroup());
            return true;
          } else if (response.statusCode().equals(CssStatusCode.MapEnded)) {
            mapperEndMap.computeIfAbsent(shuffleId, (id) -> new ConcurrentSet<>()).add(mapperKey);
            return true;
          } else {
            logger.error(
              String.format("reallocatePartitionGroup failed for %s-%s-%s-%s-<PG:%s>-%s, so far %s times",
                applicationId, shuffleId, mapperId, mapperAttemptId,
                partitionGroup.partitionGroupId, partitionGroup.epochId, numFailures));
            numFailures++;
          }
        } catch (Exception e) {
          logger.warn(
            String.format("reallocatePartitionGroup failed with exception, so far %s times", numFailures), e);
          numFailures++;
        }
        TimeUnit.MILLISECONDS.sleep(waitIntervalMs);
      }
      String err_msg = String.format("reallocatePartitionGroup failed after retry %s times", reallocateFailedMaxTimes);
      logger.warn(err_msg);
      throw new IOException(err_msg);
    } catch (Exception ex) {
      logger.error(
        String.format("reallocatePartitionGroup failed with exception for %s-%s-%s-%s-<PG:%s>-%s",
          applicationId, shuffleId, mapperId, mapperAttemptId,
          partitionGroup.partitionGroupId, partitionGroup.epochId), ex);
      return false;
    } finally {
      reallocatingLock.lock();
      try {
        reallocating.remove(groupKey);
        if (!reallocating.contains(groupKey) && !reallocatingLock.hasWaiters(reallocatingAwaitCondition)) {
          pushDataAwaitCondition.signalAll();
        }
        reallocatingAwaitCondition.signalAll();
      } finally {
        reallocatingLock.unlock();
      }
    }
  }

  private byte[] addHeaderAndCompress(
      int mapperId,
      int mapperAttemptId,
      int batchId,
      byte[] originalBytes,
      int offset,
      int length) {
    Compressor compressor = compressorThreadLocal.get();
    compressor.compress(originalBytes, offset, length);
    int compressedTotalSize = compressor.getCompressedTotalSize();
    return addHeader(mapperId, mapperAttemptId, batchId, compressor.getCompressedBuffer(), 0, compressedTotalSize);
  }

  private byte[] addHeader(
      int mapperId,
      int mapperAttemptId,
      int batchId,
      byte[] bytes,
      int offset,
      int length) {
    int BATCH_HEADER_SIZE = 4 * 4;
    byte[] body = new byte[BATCH_HEADER_SIZE + length];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapperId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, mapperAttemptId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, length);
    System.arraycopy(bytes, offset, body, BATCH_HEADER_SIZE, length);
    return body;
  }

  private boolean mapperEnded(int shuffleId, int mapId, int attemptId) {
    return mapperEndMap.containsKey(shuffleId) &&
      mapperEndMap.get(shuffleId).contains(Utils.getMapperKey(shuffleId, mapId, attemptId));
  }

  private void waitUntilZeroInFlightReqs(MapperAttemptStatus status) throws IOException {
    if (status.getException() != null){
      throw status.getException();
    }

    long deltaMs = CssConf.clientMapperEndSleepDeltaMs(cssConf);
    long timeoutMs = CssConf.clientMapperEndTimeoutMs(cssConf);

    try {
      while (status.InFlightReqs.get() > 0 && timeoutMs > 0 && status.getException() == null) {
        TimeUnit.MILLISECONDS.sleep(deltaMs);
        timeoutMs -= deltaMs;
      }
    } catch (InterruptedException e) {
      status.setException(new IOException(e));
    }

    if (status.getException() != null) {
      throw status.getException();
    }
    if (status.InFlightReqs.get() > 0) {
      throw new IOException("Client MapperEnd timeout, you might need to increase css.client.mapper.end.timeout");
    }
    if (status.getException() != null) {
      throw status.getException();
    }
  }

  @Override
  public void mapperEnd(
      String applicationId,
      int shuffleId,
      int mapperId,
      int mapperAttemptId,
      int numMappers) throws IOException {
    String mapperKey = Utils.getMapperKey(shuffleId, mapperId, mapperAttemptId);
    MapperAttemptStatus mapperAttemptStatus = statusMap.computeIfAbsent(mapperKey, (s) -> new MapperAttemptStatus());

    try {
      Timer.Context mapperEndTimer = mapperEndWaitTimer.time();
      waitUntilZeroInFlightReqs(mapperAttemptStatus);
      mapperEndTimer.stop();
      List<PartitionInfo> epochList = new ArrayList<>();
      epochList.addAll(mapperAttemptStatus.writtenEpochSet);
      List<FailedPartitionInfoBatch> batchBlacklist = this.failedBatchBlacklistEnable ?
        new ArrayList<>(mapperAttemptStatus.failedBatchBlacklist) : null;
      CssRpcMessage.MapperEndResponse response = masterRpcRef.askSync(new CssRpcMessage
        .MapperEnd(applicationId, shuffleId, mapperId, mapperAttemptId, numMappers, epochList, batchBlacklist),
        ClassTag$.MODULE$.apply(CssRpcMessage.MapperEndResponse.class));
      if (response.statusCode() != CssStatusCode.Success) {
        String msg = String.format("MapperEnd for %s failed! StatusCode: %s", mapperKey, response.statusCode());
        logger.error(msg);
        throw new IOException(msg);
      }
    } finally {
      statusMap.remove(mapperKey);
    }
  }

  @Override
  public void mapperClose(String applicationId, int shuffleId, int mapperId, int mapperAttemptId) {
    String mapperKey = Utils.getMapperKey(shuffleId, mapperId, mapperAttemptId);
    MapperAttemptStatus mapperAttemptStatus = statusMap.remove(mapperKey);
    if (mapperAttemptStatus != null) {
      mapperAttemptStatus.setException(new IOException("MapperClose accidentally"));
    }
  }

  @Override
  public CssRemoteDiskEpochReader createEpochReader(
    String applicationId,
    int shuffleId,
    List<CommittedPartitionInfo> partitions,
    CssConf conf) throws IOException {
    String shuffleKey = Utils.getShuffleKey(applicationId, shuffleId);
    return new CssRemoteDiskEpochReader(
      cssConf,
      clientFactory,
      shuffleKey,
      partitions.toArray(new CommittedPartitionInfo[0]));
  }

  @Override
  public List<CommittedPartitionInfo> getPartitionInfos(
      String applicationId,
      int shuffleId,
      int[] reduceIds,
      int startMapIndex,
      int endMapIndex) throws IOException {

    triggerReadPartitionGroupsIfNeeded(applicationId, shuffleId);

    List<CommittedPartitionInfo> partitions = new ArrayList<CommittedPartitionInfo>();
    for (int reduceId : reduceIds) {
      CommittedPartitionInfo[] reducePartitions = reducerFileGroups.get(shuffleId).get(reduceId);
      if (reducePartitions != null) {
        partitions.addAll(Arrays.asList(reducePartitions));
      }
    }
    return partitions;
  }

  /**
   * Multi-threaded read partition control to avoid excessive rpc requests.
   */
  private void triggerReadPartitionGroupsIfNeeded(String applicationId, int shuffleId) throws IOException {
    if (!reducerFileGroups.containsKey(shuffleId)) {
      synchronized (reading) {
        while (reading.contains(shuffleId)) {
          try {
            reading.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOException ioe = new IOException(e);
            throw ioe;
          }
        }
        if (reducerFileGroups.get(shuffleId) == null) {
          reading.add(shuffleId);
        }
      }
      try {
        if (!reducerFileGroups.containsKey(shuffleId)) {
          getReadPartitionGroupsWithRetry(applicationId, shuffleId);
        }
      } finally {
        synchronized (reading) {
          reading.remove(shuffleId);
          reading.notifyAll();
        }
      }
    }
  }

  /**
   * Because the stage end is triggered asynchronously, when the reduceTask data is read.
   * there will be a situation where the mapStage has not completely ended.
   * and we need to continue to wait until the stage end is completed.
   */
  private void getReadPartitionGroupsWithRetry(String applicationId, int shuffleId) throws IOException {
    long waitIntervalMs = CssConf.stageEndRetryIntervalMs(cssConf);
    Timer.Context reducerFileGroupResponseTimer = reducerFileGroupsTimer.time();
    while (!(reducerFileGroups.containsKey(shuffleId) && mapperAttempts.containsKey(shuffleId))) {
      CssRpcMessage.GetReducerFileGroupsResponse response = masterRpcRef.askSync(new CssRpcMessage
        .GetReducerFileGroups(applicationId, shuffleId),
        ClassTag$.MODULE$.apply(CssRpcMessage.GetReducerFileGroupsResponse.class));

      if (response.status() == CssStatusCode.Waiting) {
        try {
          logger.info("StageEndWaiting wait {} ms for {}-{}", waitIntervalMs, applicationId, shuffleId);
          TimeUnit.MILLISECONDS.sleep(waitIntervalMs);
        } catch (InterruptedException ex) {
          logger.warn("GetReducerFileGroups await failed.", ex);
          Thread.currentThread().interrupt();
        }
      } else if (response.status() == CssStatusCode.Timeout) {
        String errMsg = String.format("StageEndTimeout for %s-%s", applicationId, shuffleId);
        throw new IOException(errMsg);
      } else if (response.status() == CssStatusCode.StageEndDataLost) {
        dataLostCounter.inc();
        logger.error("shuffle {} read partition failed for data lost", shuffleId);
        throw new IOException(String.format("shuffle %s read partition failed for data lost", shuffleId));
      } else if (response.status() == CssStatusCode.Failed) {
        logger.error("shuffle {} read partition failed for never trigger stage end", shuffleId);
        throw new IOException(
          String.format("shuffle %s read partition failed for never trigger stage end", shuffleId));
      } else if (response.status() == CssStatusCode.Success) {
        if (response.fileGroup() == null || response.fileGroup().length == 0) {
          mapperAttempts.put(shuffleId, response.attempts());
          reducerFileGroups.put(shuffleId, new ConcurrentHashMap<>());
          logger.info(String.format("get read empty partition groups for shuffleId %s", shuffleId));
        } else {
          CommittedPartitionInfo[][] reducerCommitPartitions = response.fileGroup();
          ConcurrentHashMap<Integer, CommittedPartitionInfo[]> map = new ConcurrentHashMap<>();
          for (int i = 0; i < reducerCommitPartitions.length; i++) {
            map.put(i, reducerCommitPartitions[i]);
          }
          if (response.batchBlacklist() != null) {
            batchBlacklistMap.put(shuffleId, response.batchBlacklist());
          }
          mapperAttempts.put(shuffleId, response.attempts());
          reducerFileGroups.put(shuffleId, map);
          logger.info(String.format("get read partition groups success for shuffleId %s", shuffleId));
        }
      }
    }
    reducerFileGroupResponseTimer.stop();
  }

  @Override
  public CssInputStream readPartitions(
      String applicationId,
      int shuffleId,
      int[] reduceIds,
      int startMapIndex,
      int endMapIndex) throws IOException {
    String shuffleKey = Utils.getShuffleKey(applicationId, shuffleId);
    List<CommittedPartitionInfo> partitions =
      getPartitionInfos(applicationId, shuffleId, reduceIds, startMapIndex, endMapIndex);
    int[] mapperAttemptIds = mapperAttempts.get(shuffleId);
    Set<String> failedBatchBlacklist = null;
    if (batchBlacklistMap.get(shuffleId) != null) {
      failedBatchBlacklist = batchBlacklistMap.get(shuffleId).stream().map(x ->
        String.format("%s-%s-%s-%s-%s",
          x.getReducerId(), x.getEpochId(), x.getMapId(), x.getAttemptId(), x.getBatchId()))
        .collect(Collectors.toSet());
    }
    return CssInputStream.create(
      cssConf,
      clientFactory,
      shuffleKey,
      partitions.toArray(new CommittedPartitionInfo[partitions.size()]),
      mapperAttemptIds,
      failedBatchBlacklist,
      startMapIndex,
      endMapIndex);
  }

  @Override
  public int[] getMapperAttempts(int shuffleId) {
    return mapperAttempts.get(shuffleId);
  }

  @Override
  public List<PartitionGroup> registerPartitionGroup(
      String applicationId,
      int shuffleId,
      int numMappers,
      int numPartitions,
      int maxPartitionsPerGroup) throws IOException {
    CssRpcMessage.RegisterPartitionGroupResponse response = masterRpcRef.askSync(new CssRpcMessage
      .RegisterPartitionGroup(applicationId, shuffleId, numMappers, numPartitions, maxPartitionsPerGroup),
      ClassTag$.MODULE$.apply(CssRpcMessage.RegisterPartitionGroupResponse.class));
    if (response.statusCode() == CssStatusCode.Success) {
      applyShufflePartitionGroup(shuffleId, response.partitionGroups());
      return response.partitionGroups();
    } else {
      throw new IOException("registerPartitionGroup failed");
    }
  }

  @Override
  public void unregisterShuffle(String applicationId, int shuffleId, boolean isDriver) {
    if (isDriver) {
      // Only driver need to send out unregisterShuffle
      masterRpcRef.askSync(new CssRpcMessage.UnregisterShuffle(applicationId, shuffleId),
        ClassTag$.MODULE$.apply(CssRpcMessage.UnregisterShuffleResponse.class));
      logger.info("ShuffleClient UnregisterShuffle for appId: {} shuffleId: {} success.", applicationId, shuffleId);
    }

    mapperEndMap.remove(shuffleId);
    reducerFileGroups.remove(shuffleId);
    mapperAttempts.remove(shuffleId);
    batchBlacklistMap.remove(shuffleId);
    registering.remove(shuffleId);
    reading.remove(shuffleId);
  }

  @Override
  public void registerApplication(String applicationId) {
    synchronized (appHeartBeatLock) {
      if (!appHeartBeatStarted) {
        RpcEndpointRef heartbeatRef = rpcEnv.setupEndpointRef(
          RpcAddress.fromCssURL(CssConf.masterAddress(cssConf)), RpcNameConstants.HEARTBEAT);
        this.appHeartBeatThread = new Thread(() -> {
          long appReportTimeMs = CssConf.appTimeoutMs(cssConf) / 4;
          while (!Thread.currentThread().isInterrupted()) {
            try {
              heartbeatRef.send(new CssRpcMessage.HeartbeatFromApp(applicationId));
              Thread.sleep(appReportTimeMs);
            } catch (InterruptedException ex) {
              logger.info("Application might be shutdown, interrupted heartbeat thread.");
            } catch (Exception ex) {
              logger.error(String.format("Send heartbeat to Master failed with appId: %s", applicationId), ex);
            }
          }
        });
        this.appHeartBeatThread.setDaemon(true);
        this.appHeartBeatThread.setName("ShuffleClient application heartbeat thread.");
        this.appHeartBeatThread.start();
        this.appHeartBeatStarted = true;
      }
    }
  }

  @Override
  public void shutDown() {
    if (appHeartBeatThread != null) {
      appHeartBeatThread.interrupt();
    }

    if (pushDataRetryThreadPool != null) {
      pushDataRetryThreadPool.shutdown();
    }

    if (rpcEnv != null) {
      rpcEnv.shutdown();
    }

    if (clientFactory != null) {
      clientFactory.close();
    }

    // State cleanup
    statusMap.clear();
    mapperEndMap.clear();
    reducerFileGroups.clear();
    mapperAttempts.clear();
    batchBlacklistMap.clear();
    registering.clear();
    reallocating.clear();
    reading.clear();
    limiters.clear();
    shuffleClient = null;
  }

  public TransportClientFactory getClientFactory() {
    return clientFactory;
  }
}
