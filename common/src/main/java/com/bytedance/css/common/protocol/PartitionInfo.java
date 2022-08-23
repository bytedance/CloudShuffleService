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

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

public class PartitionInfo implements Serializable {
  protected int reducerId;       // reducer partition id start from 0
  protected int epochId;         // a reducer partition could be mapped into multi epoch

  public String getEpochKey() {
    return reducerId + "-" + epochId;
  }

  public int getReducerId() {
    return reducerId;
  }

  public void setReducerId(int reducerId) {
    this.reducerId = reducerId;
  }

  public int getEpochId() {
    return epochId;
  }

  public void setEpochId(int epochId) {
    this.epochId = epochId;
  }

  public PartitionInfo(int reducerId, int epochId) {
    this.reducerId = reducerId;
    this.epochId = epochId;
  }

  public PartitionInfo(PartitionInfo partitionInfo) {
    this.reducerId = partitionInfo.reducerId;
    this.epochId = partitionInfo.epochId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionInfo)) {
      return false;
    }
    PartitionInfo o = (PartitionInfo) other;
    return reducerId == o.reducerId && epochId == o.epochId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(reducerId, epochId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("reducerId", reducerId)
      .append("epochId", epochId)
      .toString();
  }
}
