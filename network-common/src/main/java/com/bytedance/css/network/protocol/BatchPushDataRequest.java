/*
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
 *
 * This file may have been modified by Bytedance Inc.
 */

package com.bytedance.css.network.protocol;

import com.bytedance.css.network.buffer.ManagedBuffer;
import com.bytedance.css.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public final class BatchPushDataRequest extends AbstractMessage implements RequestMessage {

  // applicationId-shuffleId
  public final String shuffleKey;
  public int[] reducerIds;
  public int epochId;
  public int[] offsets;
  public int mapperId;
  public long clientStartTime;
  public long requestId;

  // extra header that need to be used in writer & partition lazy creation
  public int replicaIndex;
  public String shuffleMode; // 0 for disk 1 for hdfs
  public String epochRotateThreshold;

  public BatchPushDataRequest(
      String shuffleKey, int[] reducerIds, int epochId, int[] offsets, int mapperId,
      int replicaIndex, String shuffleMode, String epochRotateThreshold, long clientStartTime,
      ManagedBuffer body) {
    this(shuffleKey, reducerIds, epochId, offsets, mapperId, replicaIndex,
      shuffleMode, epochRotateThreshold, clientStartTime, 0L, body);
  }

  public BatchPushDataRequest(
      String shuffleKey, int[] reducerIds, int epochId, int[] offsets, int mapperId,
      int replicaIndex, String shuffleMode, String epochRotateThreshold, long clientStartTime,
      long requestId, ManagedBuffer body) {
    super(body, true);
    this.shuffleKey = shuffleKey;
    this.reducerIds = reducerIds;
    this.epochId = epochId;
    this.offsets = offsets;
    this.mapperId = mapperId;
    this.shuffleMode = shuffleMode;
    this.replicaIndex = replicaIndex;
    this.epochRotateThreshold = epochRotateThreshold;
    this.clientStartTime = clientStartTime;
    this.requestId = requestId;
  }

  @Override
  public Type type() {
    return Type.BatchPushDataRequest;
  }

  @Override
  public int encodedLength() {
    return  8 + 8 + 4 + 4 + 4 +
      Encoders.IntArrays.encodedLength(reducerIds) +
      Encoders.IntArrays.encodedLength(offsets) +
      Encoders.Strings.encodedLength(shuffleKey) +
      Encoders.Strings.encodedLength(shuffleMode) +
      Encoders.Strings.encodedLength(epochRotateThreshold);
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.IntArrays.encode(buf, reducerIds);
    buf.writeInt(epochId);
    Encoders.IntArrays.encode(buf, offsets);
    buf.writeInt(mapperId);
    buf.writeInt(replicaIndex);
    Encoders.Strings.encode(buf, shuffleMode);
    Encoders.Strings.encode(buf, epochRotateThreshold);
    buf.writeLong(clientStartTime);
  }

  public static BatchPushDataRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    String shuffleKey = Encoders.Strings.decode(buf);
    int[] reducerIds = Encoders.IntArrays.decode(buf);
    int epochId = buf.readInt();
    int[] offsets = Encoders.IntArrays.decode(buf);
    int mapperId = buf.readInt();
    int replicaIndex = buf.readInt();
    String shuffleMode = Encoders.Strings.decode(buf);
    String epochRotateThreshold = Encoders.Strings.decode(buf);
    long clientStartTime = buf.readLong();
    return new BatchPushDataRequest(shuffleKey, reducerIds, epochId, offsets, mapperId, replicaIndex,
      shuffleMode, epochRotateThreshold, clientStartTime, requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, reducerIds, epochId, offsets, mapperId, replicaIndex, requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BatchPushDataRequest) {
      BatchPushDataRequest o = (BatchPushDataRequest) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("shuffleKey", shuffleKey)
      .append("reducerIds", reducerIds)
      .append("epochId", epochId)
      .append("offsets", offsets)
      .append("mapperId", mapperId)
      .append("replicaIndex", replicaIndex)
      .append("clientStartTime", clientStartTime)
      .append("requestId", requestId)
      .append("shuffleMode", shuffleMode)
      .append("epochRotateThreshold", epochRotateThreshold)
      .toString();
  }
}
