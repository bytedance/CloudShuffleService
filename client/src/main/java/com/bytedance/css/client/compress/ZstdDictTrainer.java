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

package com.bytedance.css.client.compress;

import com.bytedance.css.common.ChildFirstURLClassLoader;
import com.bytedance.css.common.util.Utils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;

public class ZstdDictTrainer {

  private byte[] dict;

  public ZstdDictTrainer() {
    initZstdDict("xml");
  }

  private void initZstdDict(String dictFile) {
    try {
      String jarFile = ZstdDictTrainer.class.getProtectionDomain().getCodeSource().getLocation().getFile();
      URL url = new URL(String.format("file:%s", jarFile));
      URL[] urls = {url};
      ClassLoader classLoader = new ChildFirstURLClassLoader(urls, Thread.currentThread().getContextClassLoader());
      Class zstdDictTrainerClass = classLoader.loadClass("com.github.luben.zstd.ZstdDictTrainer");
      Object trainer = zstdDictTrainerClass.getConstructor(int.class, int.class).newInstance(1024 * 1024, 32 * 1024);
      Method addSampleMethod = zstdDictTrainerClass.getDeclaredMethod("addSample", byte[].class);
      Method trainSamplesMethod = zstdDictTrainerClass.getDeclaredMethod("trainSamples", boolean.class);

      Class zstdClass = classLoader.loadClass("com.github.luben.zstd.Zstd");
      Method getDictIdFromDictMethod = zstdClass.getDeclaredMethod("getDictIdFromDict", byte[].class);

      String path = Utils.getClassLoader().getResource(dictFile).getPath();
      File file = new File(path);
      InputStream in = new FileInputStream(file);

      int count = 0;
      while (count < file.length()) {
        byte[] buffer = new byte[1024];
        IOUtils.readFully(in, buffer);
        addSampleMethod.invoke(trainer, buffer);
        count += buffer.length;
      }
      dict = (byte[]) trainSamplesMethod.invoke(trainer, true);
      assert((long) getDictIdFromDictMethod.invoke(null, dict) != 0L);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getDict() {
    return this.dict;
  }
}
