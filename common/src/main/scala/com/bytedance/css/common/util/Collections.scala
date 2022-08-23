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

package com.bytedance.css.common.util

import java.util
import java.util.function

private[css] class WrappedMap[K, V](map: util.Map[K, V]) {
  /**
   * Implicit function for computeIfAbsent
   */
  def computeWhenAbsent(key: K, fun: K => V): V = {
    map.computeIfAbsent(key, new function.Function[K, V] {
      override def apply(t: K): V = {
        fun(key)
      }
    })
  }
}

object Collections {
  implicit def wrapMap[K, V](map: util.Map[K, V]): WrappedMap[K, V] = {
    new WrappedMap(map)
  }
}
