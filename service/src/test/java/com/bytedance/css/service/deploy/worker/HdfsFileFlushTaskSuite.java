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
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class HdfsFileFlushTaskSuite {

  private String tmp = "HdfsFileFlushTaskSuite maybe let me do it !";

  @Test
  public void testFlush() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());
    long length = buffer.readableBytes();

    File file = File.createTempFile("HdfsFileFlushTaskSuite", "testFlush");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    Path path = new Path(String.format("file://%s", file.getAbsolutePath()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream outputStream = fs.create(path);

    HdfsFileFlushTask task = new HdfsFileFlushTask(buffer, notifier, outputStream);
    task.flush();
    outputStream.close();
    fs.close();
    assertFalse(notifier.hasException());
    assertEquals(file.length(), length);
  }

  @Test
  public void testFlushWithError() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());

    File file = File.createTempFile("HdfsFileFlushTaskSuite", "testFlush");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    Path path = new Path(String.format("file://%s", file.getAbsolutePath()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream outputStream = fs.create(path);
    // close outputStream in order to trigger flush error
    outputStream.close();
    fs.close();

    HdfsFileFlushTask task = new HdfsFileFlushTask(buffer, notifier, outputStream);
    task.flush();
    assertTrue(notifier.hasException());
  }

}
