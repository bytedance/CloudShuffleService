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

package com.bytedance.css.service.deploy.worker

import java.lang.management.ManagementFactory

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.util.Utils
import com.bytedance.css.service.deploy.common.BaseSource
import com.codahale.metrics.{Gauge, MetricSet}
import com.codahale.metrics.jvm.{BufferPoolMetricSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet}

class WorkerSource(
    namespace: String,
    serverId: String)
  extends BaseSource(namespace, serverId) {

  override def sourceName: String = ""

  // Gauge for exists worker instance
  metricRegistry.register(cssMetricsPrefix + s"#worker.exists",
    new Gauge[Int] { override def getValue: Int = 1 }
  )

  // Worker Heap Memory metrics.
  // when use scala-2.11 in com.codahale.metrics.MetricRegistry
  // registerAll(java.lang.String,com.codahale.metrics.MetricSet) has private access
  try {
    val clz = Utils.classForName("com.codahale.metrics.MetricRegistry")
    val registerAllMethod = clz.getDeclaredMethod("registerAll", classOf[String], classOf[MetricSet])
    registerAllMethod.setAccessible(true)
    registerAllMethod.invoke(metricRegistry, cssMetricsPrefix + s"#jvm.gc", new GarbageCollectorMetricSet)
    registerAllMethod.invoke(metricRegistry, cssMetricsPrefix + s"#jvm.heap.memory", new MemoryUsageGaugeSet)
    registerAllMethod.invoke(metricRegistry, cssMetricsPrefix + s"#jvm.buffer.pool",
      new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
  } catch {
    case t: Throwable =>
      throw new RuntimeException(t)
  }

  // WorkerLostEvent
  val WORKER_LOST_EVENT = metricRegistry.meter(cssMetricsPrefix + s"|worker=$serverId#worker.lost.event")

  // CommitFiles Qps & Latency
  val COMMIT_FILES_QPS = getEventQPS("CommitFiles")
  val COMMIT_FILES_LATENCY = getEventLatency("CommitFiles")

  // CloseFile Qps & Latency
  val CLOSE_FILE_QPS = getEventQPS("CloseFile")
  val CLOSE_FILE_LATENCY = getEventLatency("CloseFile")
}

object WorkerSource extends Logging {

  // only for test
  private val dummyWorkerSource: WorkerSource = new WorkerSource("dummy", "test")

  @volatile var workerSource: WorkerSource = dummyWorkerSource

  def create(namespace: String, serverId: String): WorkerSource = synchronized {
    if (workerSource == null || workerSource == dummyWorkerSource) {
      workerSource = new WorkerSource(namespace, serverId)
      logInfo(s"${workerSource.getClass.getName} use cssMetricsPrefix ${workerSource.cssMetricsPrefix}")
    }
    workerSource
  }
}
