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

import com.bytedance.css.common.CssConf;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;

public class CompressorSuite {

  private byte[] dict;

  @Before
  public void beforeEach() {
    ZstdDictTrainer zstdDictTrainer = new ZstdDictTrainer();
    this.dict = zstdDictTrainer.getDict();
  }

  @Test
  public void testLz4() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    Compressor compressor = new Lz4Compressor(cssConf);
    Decompressor decompressor = new Lz4Decompressor();

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }

  @Test
  public void testLz4WithRawMethod() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    cssConf.set("css.compression.test.mode", "true");
    Compressor compressor = new Lz4Compressor(cssConf);
    Decompressor decompressor = new Lz4Decompressor();

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      // raw method. compress data = origin data.
      assertEquals(source.length, (compressor.getCompressedTotalSize() - CssCompressorTrait.HEADER_LENGTH));
      byte[] raw_array = new byte[source.length];
      System.arraycopy(compressor.getCompressedBuffer(), CssCompressorTrait.HEADER_LENGTH,
        raw_array, 0, raw_array.length);
      assertEquals(context, new String(raw_array));

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }

  @Test
  public void testZstd() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    Compressor compressor = new ZstdCompressor(cssConf);
    Decompressor decompressor = new ZstdDecompressor();

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }

  @Test
  public void testZstdWithRawMethod() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    cssConf.set("css.compression.test.mode", "true");
    Compressor compressor = new ZstdCompressor(cssConf);
    Decompressor decompressor = new ZstdDecompressor();

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      // raw method. compress data = origin data.
      assertEquals(source.length, (compressor.getCompressedTotalSize() - CssCompressorTrait.HEADER_LENGTH));
      byte[] raw_array = new byte[source.length];
      System.arraycopy(compressor.getCompressedBuffer(), CssCompressorTrait.HEADER_LENGTH,
        raw_array, 0, raw_array.length);
      assertEquals(context, new String(raw_array));

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      // raw method. compress data = origin data.
      assertEquals(source.length, (compressor.getCompressedTotalSize() - CssCompressorTrait.HEADER_LENGTH));
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }

  @Test
  public void testZstdDict() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    Compressor compressor = new ZstdDictCompressor(cssConf, dict);
    Decompressor decompressor = new ZstdDictDecompressor(dict);

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }

  @Test
  public void testZstdDictWithRawMethod() throws IOException {
    CssConf cssConf = new CssConf();
    cssConf.set("css.push.buffer.size", "1024");
    cssConf.set("css.compression.test.mode", "true");
    Compressor compressor = new ZstdDictCompressor(cssConf, dict);
    Decompressor decompressor = new ZstdDictDecompressor(dict);

    for (int i = 800; i < 2000; i++) {
      String context = RandomStringUtils.randomAlphanumeric(i, i);
      byte[] source = context.getBytes();
      compressor.compress(source, 0, source.length);
      byte[] compressdBytes = compressor.getCompressedBuffer();

      // raw method. compress data = origin data.
      assertEquals(source.length, (compressor.getCompressedTotalSize() - CssCompressorTrait.HEADER_LENGTH));
      byte[] raw_array = new byte[source.length];
      System.arraycopy(compressor.getCompressedBuffer(), CssCompressorTrait.HEADER_LENGTH,
        raw_array, 0, raw_array.length);
      assertEquals(context, new String(raw_array));

      byte[] dist = new byte[decompressor.getOriginalLen(compressdBytes)];
      int limit = decompressor.decompress(compressor.getCompressedBuffer(), dist, 0);
      assertEquals(decompressor.getOriginalLen(compressdBytes), limit);
      assertEquals(context, new String(dist));
    }
  }
}
