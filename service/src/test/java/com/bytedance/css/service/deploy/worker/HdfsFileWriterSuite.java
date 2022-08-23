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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class HdfsFileWriterSuite {

  private FileFlusherImpl flusher = null;

  @Before
  public void createDiskFileFlusher() throws Exception {
    flusher = new FileFlusherImpl("HdfsFileWriterTest", FileFlusher.HDFS_FLUSHER_TYPE, 4, 128 * 1024);
  }

  @Test
  public void testSplitChunkValid() throws Exception {
    File file = File.createTempFile("HdfsFileWriterSuite", "testSplitChunkValid");
    file.deleteOnExit();

    Path path = new Path(String.format("file://%s", file.getAbsolutePath()));
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FSDataOutputStream outputStream = fs.create(path);

    HdfsFileWriter writer = new HdfsFileWriter(fs, path, outputStream, flusher, 30000, 128 * 1024, 512 * 1024 * 1024L);

    Random rand = new Random();
    AtomicLong fileLength = new AtomicLong(0);
    Thread normalThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 1000; i ++) {
          int length = 1 + rand.nextInt(16 * 1024);
          fileLength.addAndGet(length);
          byte[] tmp = new byte[length];
          tmp[length - 1] = 77; // magic num to check chunk split boundary
          ByteBuf buf = Unpooled.wrappedBuffer(tmp);
          try {
            writer.write(buf, 0);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    };

    Thread giantThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 100; i ++) {
          int length = 128 * 1024 + rand.nextInt(100) + 1;
          fileLength.addAndGet(length);
          byte[] tmp = new byte[length];
          ByteBuf buf = Unpooled.wrappedBuffer(tmp);
          try {
            writer.write(buf, 0);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    };

    normalThread.start();
    giantThread.start();
    normalThread.join();
    giantThread.join();

    writer.close();
    outputStream.close();
    fs.close();
    assertEquals(file.length(), fileLength.get());
  }
}
