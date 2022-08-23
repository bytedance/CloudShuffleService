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

package com.bytedance.css.common.metrics;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class ByteDanceMetricsReporter extends ScheduledReporter {

  private static final Logger logger = LoggerFactory.getLogger(ByteDanceMetricsEmitter.class);

  private final ByteDanceMetricsEmitter emitter;

  public ByteDanceMetricsReporter(
      ByteDanceMetricsEmitter emitter,
      MetricRegistry registry, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit) {
    super(registry, "bytedance-reporter", filter, rateUnit, durationUnit);
    this.emitter = emitter;
  }

  private String format(Object o) {
    if (o instanceof Float) {
      return format(((Float) o).doubleValue());
    } else if (o instanceof Double) {
      return format(((Double) o).doubleValue());
    } else if (o instanceof Byte) {
      return format(((Byte) o).longValue());
    } else if (o instanceof Short) {
      return format(((Short) o).longValue());
    } else if (o instanceof Integer) {
      return format(((Integer) o).longValue());
    } else if (o instanceof Long) {
      return format(((Long) o).longValue());
    }
    return null;
  }

  private String format(long n) {
    return Long.toString(n);
  }

  private String format(double v) {
    // the Carbon plaintext format is pretty underspecified, but it seems like it just wants
    // US-formatted digits
    return String.format(Locale.US, "%2.2f", v);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      emitter.emitStore(entry.getKey(), format(entry.getValue().getValue()));
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      emitter.emitStore(entry.getKey(), format(entry.getValue().getCount()));
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      Histogram histogram = entry.getValue();
      emitter.emitStore(entry.getKey() + ".counter", format(histogram.getCount()));
      Snapshot snapshot = histogram.getSnapshot();
      if (snapshot.size() > 0) {
        emitter.emitStore(entry.getKey() + ".max", format(snapshot.getMax()));
        emitter.emitStore(entry.getKey() + ".mean", format(snapshot.getMean()));
        emitter.emitStore(entry.getKey() + ".min", format(snapshot.getMin()));
        emitter.emitStore(entry.getKey() + ".pct50", format(snapshot.getMedian()));
        emitter.emitStore(entry.getKey() + ".pct75", format(snapshot.get75thPercentile()));
        emitter.emitStore(entry.getKey() + ".pct95", format(snapshot.get95thPercentile()));
        emitter.emitStore(entry.getKey() + ".pct99", format(snapshot.get99thPercentile()));
      }
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      Meter meter = entry.getValue();
      emitter.emitStore(entry.getKey() + ".counter",
        format(meter.getCount()));
      emitter.emitStore(entry.getKey() + ".m1_rate",
        format(convertRate(meter.getOneMinuteRate())));
      emitter.emitStore(entry.getKey() + ".m5_rate",
        format(convertRate(meter.getFiveMinuteRate())));
      emitter.emitStore(entry.getKey() + ".m15_rate",
        format(convertRate(meter.getFifteenMinuteRate())));
      emitter.emitStore(entry.getKey() + ".mean_rate",
        format(convertRate(meter.getMeanRate())));
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      Timer timer = entry.getValue();
      Snapshot snapshot = timer.getSnapshot();
      if (snapshot.size() > 0) {
        emitter.emitStore(entry.getKey() + ".max",
          format(convertDuration(snapshot.getMax())));
        emitter.emitStore(entry.getKey() + ".mean",
          format(convertDuration(snapshot.getMean())));
        emitter.emitStore(entry.getKey() + ".min",
          format(convertDuration(snapshot.getMin())));
        emitter.emitStore(entry.getKey() + ".p50",
          format(convertDuration(snapshot.getMedian())));
        emitter.emitStore(entry.getKey() + ".p75",
          format(convertDuration(snapshot.get75thPercentile())));
        emitter.emitStore(entry.getKey() + ".p95",
          format(convertDuration(snapshot.get95thPercentile())));
        emitter.emitStore(entry.getKey() + ".p99",
          format(convertDuration(snapshot.get99thPercentile())));
      }
    }
  }
}
