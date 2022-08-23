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

package com.bytedance.css.client.stream.hdfs;

import com.bytedance.css.client.stream.CssInputStreamImpl;
import com.bytedance.css.client.stream.EpochInputStreamBuilder;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

public class EpochHdfsInputStreamBuilder implements EpochInputStreamBuilder {

  private static final Logger logger = LoggerFactory.getLogger(CssInputStreamImpl.class);

  private FileSystem fs;

  public EpochHdfsInputStreamBuilder(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public DataInputStream createInputStream(List<CommittedPartitionInfo> partitions) throws IOException {
    DataInputStream inputStream = null;
    // could be multi hdfs file for single epoch, just pick the first one that could be opened.
    for (CommittedPartitionInfo partition : partitions) {
      try {
        Path path = new Path(partition.getFilePath());
        inputStream = fs.open(path);
        break;
      } catch (Exception ex) {
        logger.warn(String.format("Create input stream failed for hdfs %s.", partition.getFilePath()), ex);
      }
    }
    return inputStream;
  }
}
