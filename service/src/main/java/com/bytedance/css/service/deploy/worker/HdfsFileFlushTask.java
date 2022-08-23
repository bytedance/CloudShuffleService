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

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsFileFlushTask extends FlushTask {

  private static final Logger logger = LoggerFactory.getLogger(HdfsFileFlushTask.class);

  private final FSDataOutputStream outputStream;
  private final ByteBuf data;

  public HdfsFileFlushTask(ByteBuf data, FileNotifier notifier, FSDataOutputStream outputStream) {
    super(data, notifier);
    this.outputStream = outputStream;
    this.data = data;
  }

  @Override
  public void flush() {
    if (!hasException()) {
      try {
        Timer.Context timer = FileWriterMetrics.instance().hdfsFlushLatency.time();
        data.getBytes(data.readerIndex(), outputStream, data.readableBytes());
        timer.stop();
      } catch (IOException ex) {
        FileWriterMetrics.instance().hdfsFlushFailed.mark();
        logger.error("HdfsFileFlushTask flushed failed", ex);
        getNotifier().setException(ex);
      } finally {
        if (data != null) {
          data.release();
        }
      }
    }
  }
}
