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

package com.bytedance.css.network.server;

import com.bytedance.css.network.buffer.FileSegmentManagedBuffer;
import com.bytedance.css.network.buffer.ManagedBuffer;
import com.bytedance.css.network.util.TransportConf;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public final class CssManagedBufferIterator implements Iterator<ManagedBuffer> {
  private final File file;
  private final long[] offsets;
  private final int numChunks;
  private final TransportConf conf;

  private int index = 0;

  public CssManagedBufferIterator(CssFileInfo fileInfo, TransportConf conf) throws IOException {
    this.file = fileInfo.file;
    this.conf = conf;
    this.numChunks = fileInfo.numChunks;
    this.offsets = new long[numChunks + 1];
    for (int i = 0; i <= numChunks; i++) {
      offsets[i] = fileInfo.chunkOffsets.get(i);
    }
    if (offsets[numChunks] != fileInfo.fileLength) {
      throw new IOException(
        String.format("The last chunk offset %d should be equals to file length %d!",
          offsets[numChunks], fileInfo.fileLength));
    }
  }

  public void setInitIndex(int index) {
    this.index = index;
  }

  @Override
  public boolean hasNext() {
    return index < numChunks;
  }

  @Override
  public ManagedBuffer next() {
    final long offset = offsets[index];
    final long length = offsets[index + 1] - offset;
    index++;
    return new FileSegmentManagedBuffer(conf, file, offset, length);
  }
}
