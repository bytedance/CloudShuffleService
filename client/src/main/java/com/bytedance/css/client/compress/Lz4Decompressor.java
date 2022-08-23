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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.log4j.Logger;

import java.util.zip.Checksum;

public class Lz4Decompressor implements Decompressor {

  private static final Logger logger = Logger.getLogger(Lz4Decompressor.class);

  private final LZ4FastDecompressor decompressor;
  private final Checksum checksum;

  public Lz4Decompressor() {
    decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
  }

  @Override
  public int getOriginalLen(byte[] src) {
    return readIntLE(src, MAGIC_LENGTH + 5);
  }

  @Override
  public int decompress(byte[] src, byte[] dst, int dstOff) {
    int token = src[MAGIC_LENGTH] & 0xFF;
    int compressionMethod = token & 0xF0;
    int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
    int compressedLen = readIntLE(src, MAGIC_LENGTH + 1);
    int originalLen = readIntLE(src, MAGIC_LENGTH + 5);
    int check = readIntLE(src, MAGIC_LENGTH + 9);

    switch (compressionMethod) {
      case COMPRESSION_METHOD_RAW:
        System.arraycopy(src, HEADER_LENGTH, dst, dstOff, originalLen);
        break;
      case COMPRESSION_METHOD_CSS:
        int compressedLen2 = decompressor.decompress(src, HEADER_LENGTH, dst, dstOff, originalLen);
        if (compressedLen != compressedLen2) {
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
  }

  public static int readIntLE(byte[] buf, int i) {
    return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) |
      ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
  }
}
