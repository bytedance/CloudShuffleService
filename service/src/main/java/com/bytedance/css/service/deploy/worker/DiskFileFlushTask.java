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

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import com.codahale.metrics.Timer;

public class DiskFileFlushTask extends FlushTask {

  private static final Logger logger = LoggerFactory.getLogger(DiskFileFlushTask.class);

  private final FileChannel fileChannel;
  private final ByteBuf data;

  public DiskFileFlushTask(ByteBuf data, FileNotifier notifier, FileChannel fileChannel) {
    super(data, notifier);
    this.fileChannel = fileChannel;
    this.data = data;
  }

  @Override
  public void flush() {
    try {
      if (!hasException()) {
        Timer.Context timer = FileWriterMetrics.instance().diskFlushLatency.time();
        while (data.readableBytes() > 0) {
          data.readBytes(fileChannel, data.readableBytes());
        }
        // compositeBuf need to copy to HeapByteBuffer
        // cost extra cpu time
        // fileChannel.write(data.nioBuffer());
        timer.stop();
      }
    } catch (IOException ex) {
      FileWriterMetrics.instance().diskFlushFailed.mark();
      logger.error("DiskFileFlushTask flushed failed", ex);
      getNotifier().setException(ex);
    } finally {
      if (data != null) {
        ReferenceCountUtil.safeRelease(data);
      }
    }
  }
}
