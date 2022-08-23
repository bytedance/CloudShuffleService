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

public class WorkerAddress implements Serializable {

  public String host;
  public int port;

  public WorkerAddress(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WorkerAddress) {
      WorkerAddress o = (WorkerAddress) other;
      return host == o.host && port == o.port && super.equals(o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, port);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("host", host)
      .append("port", port)
      .toString();
  }
}
