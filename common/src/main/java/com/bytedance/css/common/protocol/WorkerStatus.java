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

public class WorkerStatus {

  public String name;
  public String host;
  public int rpcPort;
  public int pushPort;
  public int fetchPort;
  public long rttAvgStat;
  public long lastHeartbeat;

  public WorkerStatus() {
  }

  public WorkerStatus(
      String name,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      long rttAvgStat,
      long lastHeartbeat) {
    this.name = name;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.rttAvgStat = rttAvgStat;
    this.lastHeartbeat = lastHeartbeat;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getPushPort() {
    return pushPort;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = pushPort;
  }

  public int getFetchPort() {
    return fetchPort;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = fetchPort;
  }

  public long getRttAvgStat() {
    return rttAvgStat;
  }

  public void setRttAvgStat(long rttAvgStat) {
    this.rttAvgStat = rttAvgStat;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }
}
