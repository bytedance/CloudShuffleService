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

import java.util.List;

public class PartitionGroup extends ReplicaBase {

  public int partitionGroupId;
  public int epochId;
  public int startPartition;
  public int endPartition;

  public PartitionGroup(
      int partitionGroupId,
      int epochId,
      int startPartition,
      int endPartition,
      List<WorkerAddress> workerLists) {
    this.partitionGroupId = partitionGroupId;
    this.epochId = epochId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.replicaWorkers = workerLists;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionGroup)) {
      return false;
    }
    PartitionGroup o = (PartitionGroup) other;
    return partitionGroupId == o.partitionGroupId && epochId == o.epochId &&
      startPartition == o.startPartition && endPartition == o.endPartition;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("partitionGroupId", partitionGroupId)
      .append("epochId", epochId)
      .append("startPartition", startPartition)
      .append("endPartition", endPartition)
      .append("replicaWorkers", replicaWorkers)
      .toString();
  }
}
