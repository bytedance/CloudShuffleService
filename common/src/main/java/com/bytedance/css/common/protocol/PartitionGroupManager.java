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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionGroupManager {

  private int groupLength = -1;
  private final ConcurrentHashMap<Integer, PartitionGroup> partitionGroupMap = new ConcurrentHashMap<>();

  public PartitionGroupManager(List<PartitionGroup> partitionGroups) {
    partitionGroups.stream().forEach(p -> {
      partitionGroupMap.put(p.partitionGroupId, p);
      if (p.startPartition == 0) {
        this.groupLength = p.endPartition - p.startPartition;
      }
    });
    if (groupLength == -1) {
      throw new RuntimeException("partitionGroups could be wrong.");
    }
  }

  public Integer groupId(int reducerId) {
    return reducerId / groupLength;
  }

  public PartitionGroup getGroup(int groupId) {
    return partitionGroupMap.get(groupId);
  }

  public void updateGroup(int groupId, PartitionGroup p) {
    partitionGroupMap.put(groupId, p);
  }

  public int getGroupLength() {
    return groupLength;
  }

  public int getPartitionGroupSize() {
    return partitionGroupMap.size();
  }
}
