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

package com.bytedance.css.client.stream.disk;

import com.bytedance.css.client.stream.EpochInputStreamBuilder;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.common.util.Utils;
import com.bytedance.css.network.client.TransportClientFactory;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class EpochDiskInputStreamBuilder implements EpochInputStreamBuilder {

  private TransportClientFactory clientFactory;
  private String shuffleKey;
  private CssConf conf;

  public EpochDiskInputStreamBuilder(
      TransportClientFactory clientFactory,
      String shuffleKey,
      CssConf conf) {
    this.clientFactory = clientFactory;
    this.shuffleKey = shuffleKey;
    this.conf = conf;
  }

  @Override
  public DataInputStream createInputStream(List<CommittedPartitionInfo> partitions) throws IOException {
    // locality check for replica file. check if it can be read from local disk first
    CommittedPartitionInfo localPartitionInfo = partitions.stream()
      .filter(partition -> Utils.isLocalBlockFetchable(CssConf.localChunkFetchEnable(conf), partition.getFilePath()))
      .findFirst().orElse(null);
    if (localPartitionInfo != null) {
      return new DataInputStream(new CssLocalDiskEpochReader(shuffleKey, localPartitionInfo));
    } else {
      return
        new DataInputStream(
          new CssRemoteDiskEpochInputStream(
            new CssRemoteDiskEpochReader(
              conf,
              clientFactory,
              shuffleKey,
              partitions.toArray(new CommittedPartitionInfo[0])
            )
          )
        );
    }
  }

  public static class CssRemoteDiskEpochInputStream extends InputStream {

    private static final Logger logger = LoggerFactory.getLogger(CssRemoteDiskEpochInputStream.class);

    private CssRemoteDiskEpochReader currentReader;
    private ByteBuf currentChunk;

    public CssRemoteDiskEpochInputStream(CssRemoteDiskEpochReader currentReader) throws IOException {
      this.currentReader = currentReader;
      this.currentChunk = currentReader.next();
    }

    @Override
    public int read() throws IOException {
      boolean readable = readable();
      return readable ? currentChunk.readByte() & 255 : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      boolean readable = this.readable();
      if (!readable) {
        return -1;
      } else {
        currentChunk.readBytes(b, off, len);
        return len;
      }
    }

    private boolean readable() throws IOException {
      return currentChunk.isReadable() || nextChunk();
    }

    @Override
    public void close() throws IOException {
      if (currentChunk != null) {
        logger.info("Release chunk!");
        currentChunk.release();
        currentChunk = null;
      }
      if (currentReader != null) {
        logger.info("Closing reader");
        currentReader.close();
        currentReader = null;
      }
    }

    private boolean nextChunk() throws IOException {
      currentChunk.release();
      currentChunk = null;
      while (currentChunk == null && currentReader.hasNext()) {
        currentChunk = currentReader.next();
        if (!currentChunk.isReadable()) {
          currentChunk = null;
        } else {
          return true;
        }
      }

      // nextChunk and nextEpochReader EOF
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      return false;
    }

  }
}
