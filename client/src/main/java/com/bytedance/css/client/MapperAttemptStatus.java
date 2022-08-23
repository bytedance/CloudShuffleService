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

package com.bytedance.css.client;

import com.bytedance.css.common.protocol.FailedPartitionInfoBatch;
import com.bytedance.css.common.protocol.PartitionInfo;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MapperAttemptStatus {

  private static final Logger logger = LoggerFactory.getLogger(MapperAttemptStatus.class);

  public final AtomicInteger InFlightReqs = new AtomicInteger(0);
  private final AtomicInteger batchId = new AtomicInteger(0);
  private AtomicReference<IOException> exception = new AtomicReference<>();
  public final ConcurrentSet<PartitionInfo> writtenEpochSet = new ConcurrentSet<>();
  public final ConcurrentSet<FailedPartitionInfoBatch> failedBatchBlacklist = new ConcurrentSet<>();

  public MapperAttemptStatus() {}

  public int getNextBatchId() {
    return batchId.incrementAndGet();
  }

  public IOException getException() {
    return exception.get();
  }

  public void setException(IOException exception) {
    // setException should be called once
    this.exception.compareAndSet(null, exception);
  }
}
