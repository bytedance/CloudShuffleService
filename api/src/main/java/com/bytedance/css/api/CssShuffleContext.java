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

package com.bytedance.css.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Map;

public abstract class CssShuffleContext {

  private static final String CONTEXT_DEFAULT_IMPL = "com.bytedance.css.impl.CssShuffleContextImpl";
  private static final Logger logger = LoggerFactory.getLogger(CssShuffleContext.class);

  private static CssShuffleContext context = null;

  protected CssShuffleContext() {
  }

  public static CssShuffleContext get(String implClass) {
    if (context == null) {
      synchronized (CssShuffleContext.class) {
        if (context == null) {
          try {
            Class clz = Class.forName(implClass);
            Constructor<CssShuffleContext> constructor =
              (Constructor<CssShuffleContext>) clz.getDeclaredConstructor(new Class[]{});
            constructor.setAccessible(true);
            context = constructor.newInstance();
          } catch (Exception ex) {
            logger.error(String.format("CssShuffleContext reflect initialization with %s failed", implClass), ex);
          }
        }
      }
    }
    return context;
  }

  public static CssShuffleContext get() {
    return get(CONTEXT_DEFAULT_IMPL);
  }

  /**
   * try to start css master
   *
   * @param host
   * @param port
   * @param confMap
   */
  public abstract void startMaster(String host, int port, Map<String, String> confMap) throws Exception;

  /**
   * try to stop css master
   */
  public abstract void stopMaster() throws Exception;

  /**
   * return css master host, must be called after startMaster
   *
   * @return
   */
  public abstract String getMasterHost() throws Exception;

  /**
   * return css master port, must be called after startMaster
   *
   * @return
   */
  public abstract int getMasterPort() throws Exception;

  /**
   * initialize with proper worker nums if needed, must be called after startMaster
   *
   * @param numWorkers
   */
  public abstract void allocateWorkerIfNeeded(int numWorkers) throws Exception;

  /**
   * sync wait until shuffle stageEnd, and reduce task is ready to be scheduled
   *
   * @param appId
   * @param shuffleId
   */
  public abstract void waitUntilShuffleCommitted(String appId, int shuffleId) throws Exception;

  /**
   * destroy and clean up entire shuffle meta and data eagerly, mainly used for shuffle re-computation
   *
   * @param appId
   * @param shuffleId
   * @throws Exception
   */
  public abstract void eagerDestroyShuffle(String appId, int shuffleId) throws Exception;
}
