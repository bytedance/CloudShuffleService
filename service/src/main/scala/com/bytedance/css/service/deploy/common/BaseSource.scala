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

package com.bytedance.css.service.deploy.common

import scala.collection.mutable

import com.bytedance.css.common.metrics.source.Source
import com.codahale.metrics.{Meter, MetricRegistry, MetricSet, Timer}

/**
 * Server Side Base Metrics Source.
 */
abstract class BaseSource(
    namespace: String,
    serverId: String,
    baseMetricRegistry: MetricRegistry = new MetricRegistry) extends Source {

  override def metricRegistry: MetricRegistry = baseMetricRegistry

  private val qpsMap = mutable.Map[String, Meter]()
  private val latencyMap = mutable.Map[String, Timer]()

  def cssMetricsPrefix: String = s"namespace=$namespace|server=$serverId"

  def registerMetricSet(metricSet: MetricSet): Unit = {
    metricRegistry.registerAll(metricSet)
  }

  def withEventMetrics(eventName: String)(body: => Unit): Unit = {
    qpsMap(eventName).mark()
    val timer = latencyMap(eventName).time()
    body
    timer.stop()
  }

  def buildEventQPSName(eventName: String): String = {
    cssMetricsPrefix + s"#event.${eventName}.qps"
  }

  def buildEventFailedQPSName(eventName: String): String = {
    cssMetricsPrefix + s"#event.${eventName}.failed"
  }

  def buildEventLatencyName(eventName: String): String = {
    cssMetricsPrefix + s"#event.${eventName}.latency"
  }

  def buildDataThroughputName(dataType: String): String = {
    cssMetricsPrefix + s"#data.${dataType}.throughput"
  }

  def buildConnectionName(dataType: String): String = {
    cssMetricsPrefix + s"#worker.${dataType}.connection"
  }

  def buildOpenedFileName(fileType: String = ""): String = {
    if (fileType.isEmpty) {
      cssMetricsPrefix + s"#worker.opened.file"
    } else {
      cssMetricsPrefix + s"#worker.opened.${fileType}.file"
    }
  }

  def buildFlushQueueName(): String = {
    cssMetricsPrefix + s"#worker.flush.queue.size"
  }

  protected def getEventQPS(eventName: String): Meter = {
    val meter = metricRegistry.meter(buildEventQPSName(eventName))
    qpsMap.put(eventName, meter)
    meter
  }

  protected def getEventLatency(eventName: String): Timer = {
    val timer = metricRegistry.timer(buildEventLatencyName(eventName))
    latencyMap.put(eventName, timer)
    timer
  }
}
