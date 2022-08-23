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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class DiskFileWriterSuite {

  private FileFlusherImpl flusher = null;

  @Before
  public void createDiskFileFlusher() throws Exception {
    flusher = new FileFlusherImpl("DiskFileWriterTest", FileFlusher.DISK_FLUSHER_TYPE, 4, 128 * 1024);
  }

  @Test
  public void testSplitChunkValid() throws Exception {
    File file = File.createTempFile("DiskFileWriterSuite", "testSplitChunkValid");
    file.deleteOnExit();

    DiskFileWriter writer = new DiskFileWriter(file, flusher, 8 * 1024 * 1024L, 30000, 128 * 1024, 512 * 1024 * 1024L);

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

    normalThread.start();
    giantThread.start();
    normalThread.join();
    giantThread.join();

    writer.close();
    assertEquals(file.length(), fileLength.get());

    ArrayList<Long> chunkOffsets = writer.getChunkOffsets();
    int numChunks = chunkOffsets.size() - 1;
    FileInputStream inputStream = new FileInputStream(file);
    for (int i = 0; i < numChunks; i ++) {
      long start = chunkOffsets.get(i);
      long end = chunkOffsets.get(i + 1);
      int length = (int)(end - start);
      byte[] tmp = new byte[length];
      assertEquals(inputStream.read(tmp), length);
      assertEquals(tmp[length - 1], 77);
    }
    inputStream.close();
  }

  @Test
  public void testNoMemoryLeakIfWriteWithNotifyException() throws Exception {
    File file = File.createTempFile("DiskFileWriterSuite", "testNoMemoryLeakIfWriteWithNotifyException");
    file.deleteOnExit();

    DiskFileWriter writer = new DiskFileWriter(file, flusher, 8 * 1024 * 1024L, 30000, 128 * 1024, 512 * 1024 * 1024L);

    Random rand = new Random();
    AtomicLong fileLength = new AtomicLong(0);

    // first flush small buf to only write into composite buf.
    for (int i = 0; i < 100; i ++) {
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

    writer.setException();

    // write giant batch to just to flush.
    {
      int length = 1 + 129 * 1024;
      fileLength.addAndGet(length);
      byte[] tmp = new byte[length];
      tmp[length - 1] = 77; // magic num to check chunk split boundary
      ByteBuf buf = Unpooled.wrappedBuffer(tmp);
      try {
        writer.write(buf, 0);
      } catch (Exception ex) {
        assert (ex instanceof IOException);
        assert (ex.getMessage().equalsIgnoreCase("ForTestOnly"));
        ex.printStackTrace();
      }
      assert (buf.refCnt() == 1);
    }

    // write common small buf to composite.
    {
      int length = 1 + 1 * 1024;
      fileLength.addAndGet(length);
      byte[] tmp = new byte[length];
      tmp[length - 1] = 77; // magic num to check chunk split boundary
      ByteBuf buf = Unpooled.wrappedBuffer(tmp);
      try {
        writer.write(buf, 0);
      } catch (Exception ex) {
        assert (ex instanceof IOException);
        assert (ex.getMessage().equalsIgnoreCase("ForTestOnly"));
        ex.printStackTrace();
      }
      assert (buf.refCnt() == 1);
    }

    CompositeByteBuf compositeByteBuf = writer.getCompositeByteBuf();
    assert (compositeByteBuf.refCnt() == 1);

    try {
      writer.close();
    } catch (IOException e) {
      assert (e instanceof IOException);
      assert (e.getMessage().equalsIgnoreCase("ForTestOnly"));
      e.printStackTrace();
    }

    assert (compositeByteBuf.refCnt() == 0);
    assert (writer.getCompositeByteBuf() == null);

    writer.destroy();
  }

  @Test
  public void testNoMemoryLeakWithSubmitFlushTaskFullWithGiantBatch() throws Exception {
    File file = File.createTempFile("DiskFileWriterSuite", "testNoMemoryLeakIfWriteWithNotifyException");
    file.deleteOnExit();

    DiskFileWriter writer = new DiskFileWriter(file, flusher, 8 * 1024 * 1024L, 30000, 128 * 1024, 512 * 1024 * 1024L);

    Random rand = new Random();
    AtomicLong fileLength = new AtomicLong(0);

    // first flush small buf to only write into composite buf.
    for (int i = 0; i < 100; i ++) {
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

    writer.setSubmitQueueFullException(true);

    // write giant batch to just to flush. it will be failed & buf will be release.
    {
      int length = 1 + 129 * 1024;
      fileLength.addAndGet(length);
      byte[] tmp = new byte[length];
      tmp[length - 1] = 77; // magic num to check chunk split boundary
      ByteBuf buf = Unpooled.wrappedBuffer(tmp);
      try {
        writer.write(buf, 0);
      } catch (Exception ex) {
        assert (ex instanceof IOException);
        assert (ex.getMessage().startsWith("DiskFileWriter submit flush task timeout"));
        ex.printStackTrace();
      }
      assert (buf.refCnt() == 1);
    }

    CompositeByteBuf compositeByteBuf = writer.getCompositeByteBuf();
    assert (compositeByteBuf.refCnt() == 1);

    try {
      writer.close();
    } catch (IOException e) {
      assert (e instanceof IOException);
      assert (e.getMessage().startsWith("DiskFileWriter submit flush task timeout"));
      e.printStackTrace();
    }

    assert (compositeByteBuf.refCnt() == 0);
    assert (writer.getCompositeByteBuf() == null);

    writer.destroy();
  }

  @Test
  public void testNoMemoryLeakWithSubmitFlushTaskFullWithSmallBatch() throws Exception {
    File file = File.createTempFile("DiskFileWriterSuite", "testNoMemoryLeakIfWriteWithNotifyException");
    file.deleteOnExit();

    DiskFileWriter writer = new DiskFileWriter(file, flusher, 8 * 1024 * 1024L, 30000, 128 * 1024, 512 * 1024 * 1024L);

    Random rand = new Random();
    AtomicLong fileLength = new AtomicLong(0);

    // first flush small buf to only write into composite buf.
    for (int i = 0; i < 100; i ++) {
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

    writer.setSubmitQueueFullException(true);

    // write common small buf to composite.
    {
      int length = 1 + 128 * 1024 - writer.getCompositeByteBuf().readableBytes();
      fileLength.addAndGet(length);
      byte[] tmp = new byte[length];
      tmp[length - 1] = 77; // magic num to check chunk split boundary
      ByteBuf buf = Unpooled.wrappedBuffer(tmp);
      try {
        writer.write(buf, 0);
      } catch (Exception ex) {
        assert (ex instanceof IOException);
        assert (ex.getMessage().startsWith("DiskFileWriter submit flush task timeout"));
        ex.printStackTrace();
      }
      assert (buf.refCnt() == 1);
    }

    CompositeByteBuf compositeByteBuf = writer.getCompositeByteBuf();
    assert (compositeByteBuf.refCnt() == 1);

    try {
      writer.close();
    } catch (IOException e) {
      assert (e instanceof IOException);
      assert (e.getMessage().startsWith("DiskFileWriter submit flush task timeout"));
      e.printStackTrace();
    }

    assert (compositeByteBuf.refCnt() == 0);
    assert (writer.getCompositeByteBuf() == null);

    writer.destroy();
  }
}
