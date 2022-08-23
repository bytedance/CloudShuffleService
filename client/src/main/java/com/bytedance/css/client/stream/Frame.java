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

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.DataInputStream;

public class Frame {

  private int mapperId;
  private int attemptId;
  private int batchId;
  private int dataLength;
  private int frameLength;
  private DataInputStream data;

  public Frame(
      int mapperId,
      int attemptId,
      int batchId,
      int dataLength,
      int frameLength,
      DataInputStream data) {
    this.mapperId = mapperId;
    this.attemptId = attemptId;
    this.batchId = batchId;
    this.dataLength = dataLength;
    this.frameLength = frameLength;
    this.data = data;
  }

  public int getMapperId() {
    return mapperId;
  }

  public void setMapperId(int mapperId) {
    this.mapperId = mapperId;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(int attemptId) {
    this.attemptId = attemptId;
  }

  public int getBatchId() {
    return batchId;
  }

  public void setBatchId(int batchId) {
    this.batchId = batchId;
  }

  public int getDataLength() {
    return dataLength;
  }

  public void setDataLength(int dataLength) {
    this.dataLength = dataLength;
  }

  public DataInputStream getData() {
    return data;
  }

  public void setData(DataInputStream data) {
    this.data = data;
  }

  public int getFrameLength() {
    return frameLength;
  }

  public void setFrameLength(int frameLength) {
    this.frameLength = frameLength;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("mapperId", mapperId)
      .append("attemptId", attemptId)
      .append("batchId", batchId)
      .append("dataLength", dataLength)
      .append("frameLength", frameLength)
      .toString();
  }
}
