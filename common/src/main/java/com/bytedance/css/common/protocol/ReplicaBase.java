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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicaBase implements Serializable  {

  public List<WorkerAddress> replicaWorkers;

  public List<WorkerAddress> getReplicaWorkers() {
    return replicaWorkers;
  }

  public void setReplicaWorkers(List<WorkerAddress> replicaWorkers) {
    this.replicaWorkers = replicaWorkers;
  }

  public String makeReplicaAddressStr() {
    List<String> hostPortList = replicaWorkers.stream().map(x -> x.host + ":" + x.port)
      .collect(Collectors.toList());
    return StringUtils.join(hostPortList, "-");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReplicaBase that = (ReplicaBase) o;
    return Objects.equal(getReplicaWorkers(), that.getReplicaWorkers());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getReplicaWorkers());
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("replicaWorkers", replicaWorkers)
      .toString();
  }
}
