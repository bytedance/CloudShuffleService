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
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

public class DiskFileFlushTaskSuite {

  private String tmp = "DiskFileFlushTaskSuite java.nio.channels.FileChannel";

  @Test
  public void testFlush() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());
    long length = buffer.readableBytes();

    File file = File.createTempFile("DiskFileFlushTaskSuite", "testFlush");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    FileChannel channel = new FileOutputStream(file).getChannel();

    DiskFileFlushTask task = new DiskFileFlushTask(buffer, notifier, channel);
    task.flush();
    channel.close();
    assertFalse(notifier.hasException());
    assertEquals(file.length(), length);
  }

  @Test
  public void testFlushWithError() throws Exception {
    ByteBuf buffer = Unpooled.wrappedBuffer(tmp.getBytes());

    File file = File.createTempFile("DiskFileFlushTaskSuite", "testFlush");
    file.deleteOnExit();
    FileNotifier notifier = new FileNotifier();
    FileChannel channel = new FileOutputStream(file).getChannel();
    // close change in order to trigger flush error
    channel.close();

    DiskFileFlushTask task = new DiskFileFlushTask(buffer, notifier, channel);
    task.flush();
    assertTrue(notifier.hasException());
  }
}
