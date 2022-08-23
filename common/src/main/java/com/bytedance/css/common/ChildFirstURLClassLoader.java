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

package com.bytedance.css.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

public class ChildFirstURLClassLoader extends URLClassLoader {

  private static final Logger logger = Logger.getLogger(ChildFirstURLClassLoader.class);

  static {
    ClassLoader.registerAsParallelCapable();
  }

  private ParentClassLoader parent;
  private URL[] urls = null;

  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, null);
    this.urls = urls;
    this.parent = new ParentClassLoader(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      Class clazz = super.loadClass(name, resolve);
      logger.debug(String.format("loaded class %s with %s", name, StringUtils.join(urls)));
      return clazz;
    } catch (ClassNotFoundException cnf) {
      return parent.loadClass(name, resolve);
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    ArrayList<URL> urls = Collections.list(super.getResources(name));
    urls.addAll(Collections.list(parent.getResources(name)));
    return Collections.enumeration(urls);
  }

  @Override
  public URL getResource(String name) {
    URL url = super.getResource(name);
    if (url != null) {
      return url;
    } else {
      return parent.getResource(name);
    }
  }
}
