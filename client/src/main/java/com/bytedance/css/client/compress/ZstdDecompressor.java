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

import net.jpountz.xxhash.XXHashFactory;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.zip.Checksum;

/**
 * Use class reflect to get zstd method with target version.
 * since lower zstd version in spark 2.x can not support compressByteArray method.
 */
public class ZstdDecompressor implements Decompressor {

  private static final Logger logger = Logger.getLogger(ZstdDecompressor.class);

  private final Checksum checksum;
  private final ChildFirstURLClassLoader classLoader;
  private final Method decompressByteArrayMethod;

  public ZstdDecompressor() {
    try {
      String file = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
      URL url = new URL(String.format("file:%s", file));
      URL[] urls = {url};
      classLoader = new ChildFirstURLClassLoader(urls, Thread.currentThread().getContextClassLoader());
      Class zstdClass = classLoader.loadClass("com.github.luben.zstd.Zstd");
      decompressByteArrayMethod = zstdClass.getDeclaredMethod("decompressByteArray",
        byte[].class, int.class, int.class, byte[].class, int.class, int.class);

      checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public int getOriginalLen(byte[] src) {
    return readIntLE(src, MAGIC_LENGTH + 5);
  }

  @Override
  public int decompress(byte[] src, byte[] dst, int dstOff) {
    try {
      int token = src[MAGIC_LENGTH] & 0xFF;
      int compressionMethod = token & 0xF0;
      int compressionLevel = token & 0x0F;
      int compressedLen = readIntLE(src, MAGIC_LENGTH + 1);
      int originalLen = readIntLE(src, MAGIC_LENGTH + 5);
      int check = readIntLE(src, MAGIC_LENGTH + 9);

      switch (compressionMethod) {
        case COMPRESSION_METHOD_RAW:
          System.arraycopy(src, HEADER_LENGTH, dst, dstOff, originalLen);
          break;
        case COMPRESSION_METHOD_CSS:
          long tmp = (long) decompressByteArrayMethod.invoke(
            null, dst, dstOff, originalLen, src, HEADER_LENGTH, compressedLen);
          int compressedLen2 = (int) tmp;
          if (compressedLen2 != originalLen) {
            logger.error("compressed len corrupted!");
            return -1;
          }
      }

      checksum.reset();
      checksum.update(dst, dstOff, originalLen);
      if ((int) checksum.getValue() != check) {
        logger.error("checksum not equal!");
        return -1;
      }

      return originalLen;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static int readIntLE(byte[] buf, int i) {
    return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) |
      ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
  }
}
