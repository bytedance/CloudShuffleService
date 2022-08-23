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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CssCompressorFactory implements CompressorFactory {

  private static final Logger logger = LoggerFactory.getLogger(CompressorFactory.class);

  private final CssConf cssConf;
  private final CompressType compressType;

  public CssCompressorFactory(CssConf cssConf) {
    this.cssConf = cssConf;
    this.compressType = CompressType.valueOf(CssConf.compressionCodecType(cssConf));
    logger.debug("use compressor type {}", this.compressType);

    /**
     * Zstd dict mode. but we don't want to support this.
     * because dict can not satisfy all data distribution.
     *
     * {@link com.bytedance.css.client.compress.ZstdDictTrainer}
     */
  }

  @Override
  public Compressor getCompressor() {
    switch (compressType) {
      case lz4:
        return new Lz4Compressor(cssConf);
      case zstd:
        return new ZstdCompressor(cssConf);
      default:
        throw new IllegalArgumentException(String.format("not support compress type %s", compressType));
    }
  }

  @Override
  public Decompressor getDecompressor() {
    switch (compressType) {
      case lz4:
        return new Lz4Decompressor();
      case zstd:
        return new ZstdDecompressor();
      default:
        throw new IllegalArgumentException(String.format("not support decompress type %s", compressType));
    }
  }

  private enum CompressType {
    lz4("lz4"),
    zstd("zstd"),
    zstd_dict("zstd-dict");

    CompressType(String type) {
      this.type = type;
    }

    public String type;
  }
}
