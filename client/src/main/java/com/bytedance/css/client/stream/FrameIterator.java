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

package com.bytedance.css.client.stream;

import com.bytedance.css.common.exception.CssRuntimeException;
import com.bytedance.css.common.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

public class FrameIterator implements Iterator<Frame> {

  private static final Logger logger = LoggerFactory.getLogger(FrameIterator.class);

  private DataInputStream currentInputStream;
  // mapId, attemptId, batchId, size
  private final int BATCH_HEADER_SIZE;
  private final byte[] headerBuf;
  private Frame currentFrame;

  public FrameIterator() {
    BATCH_HEADER_SIZE = 4 * 4;
    headerBuf = new byte[BATCH_HEADER_SIZE];
  }

  @Override
  public boolean hasNext() {
    if (currentInputStream == null) {
      return false;
    }

    int size;
    try {
      currentInputStream.readFully(headerBuf);
      size = Platform.getInt(headerBuf, Platform.BYTE_ARRAY_OFFSET + 12);
    } catch (IOException e) {
      try {
        closeInputStream();
      } catch (IOException ex) {
        logger.error("Error occur when close inputStream", ex);
      }
      if (e instanceof EOFException) {
        return false;
      }
      throw new CssRuntimeException("Read frame data failed.", e);
    }

    int mapId = Platform.getInt(headerBuf, Platform.BYTE_ARRAY_OFFSET);
    int attemptId = Platform.getInt(headerBuf, Platform.BYTE_ARRAY_OFFSET + 4);
    int batchId = Platform.getInt(headerBuf, Platform.BYTE_ARRAY_OFFSET + 8);

    currentFrame = new Frame(mapId, attemptId, batchId, size, BATCH_HEADER_SIZE + size, currentInputStream);
    return true;
  }

  private void closeInputStream() throws IOException {
    if (currentInputStream != null) {
      currentInputStream.close();
    }
    currentInputStream = null;
  }

  @Override
  public Frame next() {
    return currentFrame;
  }

  public void resetInputStream(DataInputStream inputStream) throws IOException {
    if (this.currentInputStream != null) {
      this.currentInputStream.close();
    }
    this.currentInputStream = inputStream;
  }

  public void close() throws IOException {
    if (this.currentInputStream != null) {
      this.currentInputStream.close();
    }
    this.currentInputStream = null;
  }

}
