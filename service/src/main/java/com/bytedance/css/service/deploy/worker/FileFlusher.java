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

public abstract class FileFlusher {

  public static final String DISK_FLUSHER_TYPE = "Flush";
  public static final String HDFS_FLUSHER_TYPE = "HdfsFlush";
  public static final String DISK_REAL_FLUSHER_TYPE = "RealFlush";

  public abstract boolean submitTask(FlushTask task, long timeoutMs);

  public abstract int pendingFlushTaskNum();
}
