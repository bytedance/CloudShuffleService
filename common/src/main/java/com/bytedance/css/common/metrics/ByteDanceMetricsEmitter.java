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

package com.bytedance.css.common.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Emit metrics to bytedance metrics system.
 *
 * Metrics name format: ${tags}#${metricsName}
 */
public class ByteDanceMetricsEmitter {

  private static final Logger logger = LoggerFactory.getLogger(ByteDanceMetricsEmitter.class);

  private static final String METRICS_HOST = "localhost";
  private static final int METRICS_PORT = 9123;

  private static Map<String, ByteDanceMetricsEmitter> emitters = null;
  private static DatagramSocket socket = null;
  private static InetSocketAddress address = null;

  private final String namespace;

  private ByteDanceMetricsEmitter(String namespace) {
    this.namespace = namespace;
  }

  public static synchronized ByteDanceMetricsEmitter getEmitter(String namespace) {
    if (emitters == null) {
      emitters = new HashMap<>();
    }
    if (socket == null || address == null) {
      address = new InetSocketAddress(METRICS_HOST, METRICS_PORT);
      try {
        socket = new DatagramSocket();
      } catch (Exception e) {
        logger.debug("Failed to create DatagramSocket.", e);
      }
    }
    ByteDanceMetricsEmitter emitter = emitters.get(namespace);
    if (emitter == null) {
      emitter = new ByteDanceMetricsEmitter(namespace);
      emitters.put(namespace, emitter);
    }
    return emitter;
  }

  public static String makeTags(Map<String, String> tags) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      if (sb.length() > 0) {
        sb.append('|');
      }
      sb.append(tag.getKey());
      sb.append('=');
      sb.append(tag.getValue());
    }
    return sb.toString();
  }

  private void emit(byte[] buf) {
    try {
      DatagramPacket packet = new DatagramPacket(buf, buf.length, address);
      socket.send(packet);
    } catch (Exception e) {
      logger.debug("Failed to emit metrics.", e);
    }
  }

  private void writeMsgpackString(DataOutputStream out, String s) {
    try {
      if (s.length() < 32) {
        out.writeByte(160 + s.length()); // 4 byte string
      } else if (s.length() < (1 << 8)) {
        out.writeByte(0xd9);
        out.writeByte(s.length());
      } else if (s.length() < (1 << 16)) {
        out.writeByte(0xda);
        out.writeShort(s.length());
      } else {
        out.writeByte(0xdb);
        out.writeInt(s.length());
      }
      out.write(s.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      logger.debug("Failed to write message package.", e);
    }
  }

  private void emit(String type, String fullName, String value, String tags) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      dataOut.writeByte(144 + 6); // 6 element array
      writeMsgpackString(dataOut, "emit");
      writeMsgpackString(dataOut, type);
      writeMsgpackString(dataOut, fullName);
      writeMsgpackString(dataOut, value);
      writeMsgpackString(dataOut, tags);
      writeMsgpackString(dataOut, "");
      dataOut.flush();
      byte[] buf = out.toByteArray();
      emit(buf);
    } finally {
      dataOut.close();
      out.close();
    }
  }

  public void emitCounter(String name, String value) {
    String[] splits = name.split("#");
    if (splits.length == 2) {
      emitStore(splits[1], value, splits[0]);
    } else {
      emitCounter(name, value, "");
    }
  }

  public void emitCounter(String name, String value, String tags) {
    try {
      emit("counter", namespace + "." + name, value, tags);
    } catch (Exception e) {
      // preserve conventions
      logger.debug("Failed to emit counter.", e);
    }
  }

  public void emitTimer(String name, String value) {
    String[] splits = name.split("#");
    if (splits.length == 2) {
      emitStore(splits[1], value, splits[0]);
    } else {
      emitTimer(name, value, "");
    }
  }

  public void emitTimer(String name, String value, String tags) {
    try {
      emit("timer", namespace + "." + name, value, tags);
    } catch (Exception e) {
      // preserve conventions
      logger.debug("Failed to emit timer.", e);
    }
  }

  public void emitStore(String name, String value) {
    String[] splits = name.split("#");
    if (splits.length == 2) {
      emitStore(splits[1], value, splits[0]);
    } else {
      emitStore(name, value, "");
    }
  }

  public void emitStore(String name, String value, String tags) {
    try {
      emit("store", namespace + "." + name, value, tags);
    } catch (Exception e) {
      // preserve conventions
      logger.debug("Failed to emit store.", e);
    }
  }
}
