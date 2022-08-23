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

package com.bytedance.css.client.compress;

import com.bytedance.css.common.ChildFirstURLClassLoader;
import com.bytedance.css.common.CssConf;
import net.jpountz.xxhash.XXHashFactory;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.zip.Checksum;

/**
 * Use class reflect to get zstd method with target version.
 * since lower zstd version in spark 2.x can not support compressByteArray method.
 */
public class ZstdDictCompressor implements Compressor {

  private final Checksum checksum;
  private final int zstdCompressLevel;
  private final ChildFirstURLClassLoader classLoader;
  private final Method compressBoundMethod;
  private final Method compressUsingDictMethod;

  // only used for test
  private boolean testMode;
  private byte[] compressedBuffer;
  private int compressedTotalSize;

  private volatile byte[] dict;

  public ZstdDictCompressor(CssConf cssConf, byte[] dict) {
    try {
      String file = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
      URL url = new URL(String.format("file:%s", file));
      URL[] urls = {url};
      classLoader = new ChildFirstURLClassLoader(urls, Thread.currentThread().getContextClassLoader());
      Class zstdClass = classLoader.loadClass("com.github.luben.zstd.Zstd");
      compressBoundMethod = zstdClass.getDeclaredMethod("compressBound", long.class);
      compressUsingDictMethod = zstdClass.getDeclaredMethod("compressUsingDict",
        byte[].class, int.class, byte[].class, int.class, int.class, byte[].class, int.class);

      this.dict = dict;
      this.zstdCompressLevel = CssConf.zstdCompressionLevel(cssConf);
      this.testMode = CssConf.compressionTestMode(cssConf);
      int blockSize = (int) CssConf.pushBufferSize(cssConf);
      checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
      initCompressBuffer(blockSize);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initCompressBuffer(int blockSize) throws Exception {
    long compressedBlockSize = HEADER_LENGTH + (long) compressBoundMethod.invoke(null, blockSize);
    compressedBuffer = new byte[(int) compressedBlockSize];
    System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
  }

  @Override
  public void compress(byte[] data, int offset, int length) {
    try {
      checksum.reset();
      checksum.update(data, offset, length);
      final int check = (int) checksum.getValue();
      if (compressedBuffer.length - HEADER_LENGTH < length) {
        initCompressBuffer(length);
      }
      long tmp = (long) compressUsingDictMethod.invoke(
        null, this.compressedBuffer, HEADER_LENGTH, data, offset, length, dict, this.zstdCompressLevel);
      int compressedLength = (int) tmp;
      final int compressMethod;
      if (compressedLength >= length || testMode) {
        compressMethod = COMPRESSION_METHOD_RAW;
        compressedLength = length;
        System.arraycopy(data, offset, compressedBuffer, HEADER_LENGTH, length);
      } else {
        compressMethod = COMPRESSION_METHOD_CSS;
      }

      compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | zstdCompressLevel);
      writeIntLE(compressedLength, compressedBuffer, MAGIC_LENGTH + 1);
      writeIntLE(length, compressedBuffer, MAGIC_LENGTH + 5);
      writeIntLE(check, compressedBuffer, MAGIC_LENGTH + 9);

      compressedTotalSize = HEADER_LENGTH + compressedLength;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int getCompressedTotalSize() {
    return compressedTotalSize;
  }

  @Override
  public byte[] getCompressedBuffer() {
    return compressedBuffer;
  }

  private static void writeIntLE(int i, byte[] buf, int off) {
    buf[off++] = (byte) i;
    buf[off++] = (byte) (i >>> 8);
    buf[off++] = (byte) (i >>> 16);
    buf[off++] = (byte) (i >>> 24);
  }
}
