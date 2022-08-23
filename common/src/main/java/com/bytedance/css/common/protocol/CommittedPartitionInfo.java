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

package com.bytedance.css.common.protocol;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

public class CommittedPartitionInfo extends PartitionInfo implements Serializable {
  private String host;
  private int port;
  private ShuffleMode shuffleMode;
  private String filePath;
  // normal length; 0 length; -1L means commit failed
  private long fileLength = -1L;

  public CommittedPartitionInfo(
      int reducerId,
      int epochId,
      String host,
      int port,
      ShuffleMode shuffleMode,
      String filePath,
      long fileLength) {
    super(reducerId, epochId);
    this.host = host;
    this.port = port;
    this.shuffleMode = shuffleMode;
    this.filePath = filePath;
    this.fileLength = fileLength;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public ShuffleMode getShuffleMode() {
    return shuffleMode;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getFileLength() {
    return fileLength;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CommittedPartitionInfo)) {
      return false;
    }
    CommittedPartitionInfo o = (CommittedPartitionInfo) other;
    return super.equals(o) && host.equals(o.host) && port == o.port && shuffleMode == o.shuffleMode
      && filePath.equals(o.filePath) && fileLength == o.fileLength;
  }

  @Override
  public int hashCode() {
    return (super.hashCode() + host + port + shuffleMode + filePath + fileLength).hashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("reducerId", reducerId)
      .append("epochId", epochId)
      .append("host", host)
      .append("port", port)
      .append("shuffleMode", shuffleMode)
      .append("filePath", filePath)
      .append("fileLength", fileLength)
      .toString();
  }
}
