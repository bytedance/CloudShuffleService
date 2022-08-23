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

import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CssLocalDiskEpochReader extends InputStream {

  private static final Logger logger = LoggerFactory.getLogger(CssLocalDiskEpochReader.class);

  private final String shuffleKey;
  private final String epochKey;
  private InputStream inputStream;

  public CssLocalDiskEpochReader(String shuffleKey, CommittedPartitionInfo partition) throws IOException {
    this.shuffleKey = shuffleKey;
    this.epochKey = partition.getEpochKey();

    try {
      inputStream = new BufferedInputStream(new FileInputStream(partition.getFilePath()));
    } catch (IOException e) {
      throw new IOException(e);
    }
    logger.info(String.format("read next epoch %s for shuffle %s from local", epochKey, shuffleKey));
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      logger.info(String.format("Closing local reader for shuffle %s epoch %s", shuffleKey, epochKey));
      inputStream.close();
      inputStream = null;
    }
  }
}
