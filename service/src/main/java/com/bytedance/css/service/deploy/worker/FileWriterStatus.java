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

package com.bytedance.css.service.deploy.worker;

public enum FileWriterStatus {

  Writable(0),                  // Writable, file writer ready to write.
  WritableButShouldRotate(1),   // Writable, but needs to notify the client to Rotate, generally used for forced writing
  ShouldRotate(2),              // UnWritable, do not write data, and notify the client to Rotate
  UnWritable(3);                // UnWritable, file writer catch some exception.

  private final byte value;

  FileWriterStatus(int value) {
    assert(value >= 0 && value < 256);
    this.value = (byte) value;
  }

  public byte getValue() {
    return value;
  }
}
