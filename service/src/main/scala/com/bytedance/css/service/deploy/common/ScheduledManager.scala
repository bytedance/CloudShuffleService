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

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.util.{ThreadUtils, Utils}

/**
 * Creates a scheduled manager which will hold all scheduled task.
 *
 * @param name the name for scheduled manager.
 * @param numThreads the num thread for scheduled manager.
 */
private[deploy] class ScheduledManager(name: String, numThreads: Int) extends Logging {

  // Executor for the forward message task
  private val executors = ThreadUtils.newDaemonThreadPoolScheduledExecutor(name, numThreads)
  private val taskMap = new ConcurrentHashMap[String, TaskInfo]()

  case class TaskInfo(
      name: String,
      scheduledTask: () => Unit,
      initialDelay: Long,
      period: Long,
      var scheduledFuture: ScheduledFuture[_] = null)

  /** Schedules a task to run forward message. */
  def addScheduledTask(
      name: String,
      scheduledTask: () => Unit,
      initialDelay: Long,
      period: Long): Unit = {
    taskMap.put(name, TaskInfo(name, scheduledTask, initialDelay, period))
  }

  /** Starts the scheduled manager. */
  def start(): Unit = {
    taskMap.asScala.foreach { entry =>
      val taskName = entry._1
      val taskInfo = entry._2
      val task = new Runnable() {
        override def run(): Unit = {
          Utils.logUncaughtExceptions(taskInfo.scheduledTask())
        }
      }
      taskInfo.scheduledFuture =
        executors.scheduleAtFixedRate(task, taskInfo.initialDelay, taskInfo.period, TimeUnit.MILLISECONDS)
      logInfo(s"start scheduled task with [$taskName]")
    }
  }

  /** Stops the scheduled manager. */
  def stop(): Unit = {
    taskMap.asScala.foreach { entry =>
      val taskName = entry._1
      val taskInfo = entry._2
      if (taskInfo.scheduledFuture != null) {
        taskInfo.scheduledFuture.cancel(true)
        logInfo(s"stop scheduled task with [$taskName]")
      }
    }
    executors.shutdown()
    executors.awaitTermination(10, TimeUnit.SECONDS)
  }
}
