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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.zip.Checksum;

/**
 * Use class reflect to get zstd method with target version.
 * since lower zstd version in spark 2.x can not support compressByteArray method.
 */
public class ZstdCompressor implements Compressor {

  private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

  private final Checksum checksum;
  private final int zstdCompressLevel;
  private final ChildFirstURLClassLoader classLoader;
  private final Method compressBoundMethod;
  private final Method compressByteArrayMethod;
  private final Method compressIsErrorMethod;
  private final Method compressGetErrorNameMethod;
  private final Method compressGetErrorCodeMethod;

  // only used for test
  private boolean testMode;
  private byte[] compressedBuffer;
  private int compressedTotalSize;

  public ZstdCompressor() {
    this(new CssConf());
  }

  public ZstdCompressor(CssConf cssConf) {
    try {
      String file = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
      URL url = new URL(String.format("file:%s", file));
      URL[] urls = {url};
      classLoader = new ChildFirstURLClassLoader(urls, Thread.currentThread().getContextClassLoader());
      Class zstdClass = classLoader.loadClass("com.github.luben.zstd.Zstd");
      compressBoundMethod = zstdClass.getDeclaredMethod("compressBound", long.class);
      compressByteArrayMethod = zstdClass.getDeclaredMethod("compressByteArray",
        byte[].class, int.class, int.class, byte[].class, int.class, int.class, int.class);
      compressIsErrorMethod = zstdClass.getDeclaredMethod("isError", long.class);
      compressGetErrorNameMethod = zstdClass.getDeclaredMethod("getErrorName", long.class);
      compressGetErrorCodeMethod = zstdClass.getDeclaredMethod("getErrorCode", long.class);

      int blockSize = (int) CssConf.pushBufferSize(cssConf);
      this.zstdCompressLevel = CssConf.zstdCompressionLevel(cssConf);
      this.testMode = CssConf.compressionTestMode(cssConf);
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
      long tmp = (long) compressByteArrayMethod.invoke(
        null, compressedBuffer, HEADER_LENGTH, compressedBuffer.length - HEADER_LENGTH,
        data, offset, length, this.zstdCompressLevel);
      boolean isCompressError = ((boolean) compressIsErrorMethod.invoke(null, tmp)) | testMode;
      if (isCompressError) {
        String errorName = (String) compressGetErrorNameMethod.invoke(null, tmp);
        long errorCode = (long) compressGetErrorCodeMethod.invoke(null, tmp);
        logger.error("zstd compress error with data str {} compressLen {} errorName {} errorCode {}. ignore it.",
          new String(data, offset, length), tmp, errorName, errorCode);
      }

      int compressedLength = (int) tmp;
      final int compressMethod;
      if (isCompressError || compressedLength >= length) {
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
