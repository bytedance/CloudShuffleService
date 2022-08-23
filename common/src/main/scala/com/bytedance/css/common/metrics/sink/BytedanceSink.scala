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

package com.bytedance.css.common.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.bytedance.css.common.metrics.{ByteDanceMetricsEmitter, ByteDanceMetricsReporter, MetricsSystem}
import com.codahale.metrics.{MetricFilter, MetricRegistry}

class BytedanceSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val KEY_PERIOD = "period"
  val KEY_UNIT = "unit"
  val PREFIX = "prefix"

  val DEFAULT_PERIOD = 5
  val DEFAULT_UNIT = "SECONDS"
  val DEFAULT_PREFIX = "inf.css.v2"

  val pollPeriod = Option(property.getProperty(KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(DEFAULT_UNIT)
  }

  val prefix: String = Option(property.getProperty(PREFIX)) match {
    case Some(s) => s
    case None => DEFAULT_PREFIX
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val emitter = ByteDanceMetricsEmitter.getEmitter(prefix)
  val reporter = new ByteDanceMetricsReporter(emitter, registry, MetricFilter.ALL,
    TimeUnit.SECONDS, TimeUnit.MILLISECONDS)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
