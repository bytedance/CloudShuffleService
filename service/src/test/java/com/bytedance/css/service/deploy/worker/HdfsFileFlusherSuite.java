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
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertEquals;

public class HdfsFileFlusherSuite {

  private FileFlusherImpl flusher = null;

  @Before
  public void createHdfsFileFlusher() throws Exception {
    flusher = new FileFlusherImpl("HdfsFileFlusherTest", FileFlusher.HDFS_FLUSHER_TYPE, 4, 128 * 1024);
  }

  @Test
  public void testFlush() throws Exception {
    String tmp = "Hello-World!!!";
    ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());
    int bufferLength = buffer.readableBytes();
    File file = File.createTempFile("HdfsFileFlusherSuite", "testFlush");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    Path path = new Path(String.format("file://%s", file.getAbsolutePath()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream outputStream = fs.create(path);

    HdfsFileFlushTask task = new HdfsFileFlushTask(buffer, notifier, outputStream);
    flusher.submitTask(task, 1000);

    Thread.sleep(1000);
    outputStream.close();

    FileInputStream inputStream = new FileInputStream(file);

    byte[] bytes = new byte[bufferLength];
    String result = null;
    while (inputStream.read(bytes) != -1){
      result = new String(bytes);
    }
    inputStream.close();
    fs.close();
    assertEquals(result, tmp);
  }

  @Test
  public void testContinuousFlushTask() throws Exception {
    String tmp = "Hello-World!!! testContinuousFlushTask";
    File file = File.createTempFile("HdfsFileFlusherSuite", "testContinuousFlushTask");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    Path path = new Path(String.format("file://%s", file.getAbsolutePath()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream outputStream = fs.create(path);

    long totalFlushBytes = 0;

    for (int i = 0; i < 1000; i ++) {
      ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());
      totalFlushBytes += buffer.readableBytes();
      notifier.getNumPendingFlushes().incrementAndGet();
      HdfsFileFlushTask task = new HdfsFileFlushTask(buffer, notifier, outputStream);
      flusher.submitTask(task, 1000);
    }

    Thread.sleep(3000);
    assertEquals(notifier.getNumPendingFlushes().get(), 0);
    outputStream.close();
    fs.close();
    assertEquals(totalFlushBytes, file.length());
  }
}
