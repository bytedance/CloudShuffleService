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

package com.bytedance.css.network.protocol.shuffle;

import com.bytedance.css.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

/** Request to read a set of chunks. Returns {@link StreamHandle}. */
public class OpenStream extends BlockTransferMessage {
  public final String shuffleKey;
  public final String filePath;
  public final int initChunkIndex;

  public OpenStream(String shuffleKey, String filePath, int initChunkIndex) {
    this.shuffleKey = shuffleKey;
    this.filePath = filePath;
    this.initChunkIndex = initChunkIndex;
  }

  @Override
  protected Type type() { return Type.OPEN_STREAM; }

  @Override
  public int hashCode() {
    return Objects.hash(shuffleKey, filePath, initChunkIndex);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("shuffleKey", shuffleKey)
      .append("filePath", filePath)
      .append("initChunkIndex", initChunkIndex)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof OpenStream) {
      OpenStream o = (OpenStream) other;
      return Objects.equals(shuffleKey, o.shuffleKey)
        && Objects.equals(filePath, o.filePath)
        && Objects.equals(initChunkIndex, o.initChunkIndex);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(shuffleKey)
      + Encoders.Strings.encodedLength(filePath)
      + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, filePath);
    buf.writeInt(initChunkIndex);
  }

  public static OpenStream decode(ByteBuf buf) {
    String shuffleKey = Encoders.Strings.decode(buf);
    String filePath = Encoders.Strings.decode(buf);
    int initChunkIndex = buf.readInt();
    return new OpenStream(shuffleKey, filePath, initChunkIndex);
  }
}
