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

import com.bytedance.css.network.protocol.Encodable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;


public abstract class BlockTransferMessage implements Encodable {
  protected abstract Type type();

  /** Preceding every serialized message is its type, which allows us to deserialize it. */
  public enum Type {
    OPEN_STREAM(0), STREAM_HANDLE(1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }
  }

  // NB: Java does not support static methods in interfaces, so we must put this in a static class.
  public static class Decoder {
    /** Deserializes the 'type' byte followed by the message itself. */
    public static BlockTransferMessage fromByteBuffer(ByteBuffer msg) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg);
      byte type = buf.readByte();
      switch (type) {
        case 0: return OpenStream.decode(buf);
        case 1: return StreamHandle.decode(buf);
        default: throw new IllegalArgumentException("Unknown message type: " + type);
      }
    }
  }

  /** Serializes the 'type' byte followed by the message itself. */
  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id);
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }
}
