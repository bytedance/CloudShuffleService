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

package com.bytedance.css.client.metrics;

import com.bytedance.css.common.metrics.source.Source;
import com.codahale.metrics.MetricRegistry;

/**
 * Client Side Base Metrics Source.
 */
public abstract class BaseSource implements Source {

  protected MetricRegistry registry = new MetricRegistry();
  protected String namespace;
  protected String application;

  public BaseSource(String namespace, String application) {
    this.namespace = namespace;
    this.application = application;
  }

  public String cssMetricsPrefix() {
    return String.format("namespace=%s|applicationId=%s", namespace, application);
  }

  @Override
  public MetricRegistry metricRegistry() {
    return registry;
  }
}
