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

package com.bytedance.css.common

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.internal.config._
import com.bytedance.css.common.protocol.ShuffleMode
import com.bytedance.css.common.util.Utils

class CssConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import CssConf._

  /** Create a CssConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new CssConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  def loadFromSystemProperties(silent: Boolean): CssConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("css.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): CssConf = {
    set(key, value, false)
  }

  def set(key: String, value: String, silent: Boolean): CssConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): CssConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): CssConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): CssConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): CssConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): CssConf = {
    if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): CssConf = {
    if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): CssConf = {
    settings.remove(key)
    this
  }

  def remove(entry: ConfigEntry[_]): CssConf = {
    remove(entry.key)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
   * Retrieves the value of a pre-defined configuration entry.
   *
   * - This is an internal Spark API.
   * - The return type if defined by the configuration entry.
   * - This will throw an exception is the config is not optional and the value is not set.
   */
  def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** Get an optional value, applying variable substitution. */
  def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute(_))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }


  /**
   * Get a parameter as an integer, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as an integer
   */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a long, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as a long
   */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a double, falling back to a default if not ste
   * @throws NumberFormatException If the value cannot be interpreted as a double
   */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a boolean, falling back to a default if not set
   * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this CssConf */
  def getExecutorEnv: Seq[(String, String)] = {
    getAllWithPrefix("spark.executorEnv.")
  }

  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   */
  def getAppId: String = get("spark.app.id")

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object */
  override def clone: CssConf = {
    val cloned = new CssConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  def getenv(name: String): String = System.getenv(name)

  /**
   * Wrapper method for get() methods which require some specific value format. This catches
   * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
   * incorrectly configured key in the exception message.
   */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }
}

object CssConf extends Logging {

  /**
   * Maps deprecated config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("css.dummy.deprecatedKey", "", "")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version of Spark.
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
   *
   * TODO: consolidate it with `ConfigBuilder.withAlternative`.
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "css.dummy.alternateKey" -> Seq(
      AlternateConfig("css.dummy.alternateKey_1.0", "1.0"))
  )

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
   */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
    if (key.startsWith("spark.akka") || key.startsWith("spark.ssl.akka")) {
      logWarning(
        s"The configuration key $key is not supported anymore " +
          s"because Spark doesn't use Akka since 2.0")
    }
  }

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   *
   * @param key The deprecated key.
   * @param version Version of Spark where key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   *
   * @param key The deprecated config key.
   * @param version The Spark version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

  def workerRegisterTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.worker.register.timeout", "180s")
  }

  def flushBufferSize(conf: CssConf): Long = {
    conf.getSizeAsBytes("css.flush.buffer.size", "256k")
  }

  def flushQueueCapacity(conf: CssConf): Int = {
    conf.getInt("css.flush.queue.capacity", 512)
  }

  def flushTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.flush.timeout", "360s")
  }

  def pushThreads(conf: CssConf): Int = {
    conf.getInt("css.push.io.threads", 32)
  }

  def pushServerPort(conf: CssConf, port: Int = -1): Int = {
    if (port > 0) {
      port
    } else {
      conf.getInt("css.push.server.port", 0)
    }
  }

  def fetchThreads(conf: CssConf): Int = {
    conf.getInt("css.fetch.io.threads", 32)
  }

  def dataThreads(conf: CssConf): Int = {
    conf.getInt("css.data.io.threads", 8)
  }

  def fetchServerPort(conf: CssConf, port: Int = -1): Int = {
    if (port > 0) {
      port
    } else {
      conf.getInt("css.fetch.server.port", 0)
    }
  }

  def commitThreads(conf: CssConf): Int = {
    conf.getInt("css.commit.threads", 128)
  }

  // css.diskFlusher.base.dirs = /mnt/disk1/css,/mnt/disk2/css,/mnt/disk3/css,/mnt/disk4/css,
  // numbers of dirs will determine how many disk flusher will be.
  def diskFlusherBaseDirs(conf: CssConf): Array[String] = {
    val baseDirs = conf.get("css.diskFlusher.base.dirs",
      Option(conf.getenv("LOCAL_DIRS")).getOrElse("/tmp/css"))
    if (baseDirs.nonEmpty) {
      baseDirs.split(",").filter(!_.isEmpty)
    } else {
      Array[String]()
    }
  }

  def diskFlusherNum(conf: CssConf): Int = {
    conf.getInt("css.diskFlusher.num", -1)
  }

  def hdfsFlusherBaseDir(conf: CssConf): String = {
    conf.get("css.hdfsFlusher.base.dir", "")
  }

  def hdfsFlusherNum(conf: CssConf): Int = {
    conf.getInt("css.hdfsFlusher.num", 0)
  }

  def hdfsFlusherReplica(conf: CssConf): String = {
    conf.get("css.hdfsFlusher.replica", "2")
  }

  def workerTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.worker.timeout", "120s")
  }

  def workerLostExpireTimeMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.worker.lost.expireTime", "120s")
  }

  def appTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.app.timeout", "120s")
  }

  def removeShuffleDelayMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.remove.shuffle.delay", "60s")
  }

  def fetchChunkSize(conf: CssConf): Long = {
    conf.getSizeAsBytes("css.fetch.chunk.size", "8m")
  }

  def pushBufferSize(conf: CssConf): Long = {
    conf.getSizeAsBytes("css.push.buffer.size", "64k")
  }

  def correctnessModeEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.correctnessMode.enabled", false)
  }

  def partitionGroupPushBufferSize(conf: CssConf): Long = {
    conf.getSizeAsBytes("css.partitionGroup.push.buffer.size", "4m")
  }

  def partitionGroupPushRetries(conf: CssConf): Int = {
    conf.getInt("css.partitionGroup.push.maxRetries", 3)
  }

  def maxPartitionsPerGroup(conf: CssConf): Int = {
    conf.getInt("css.maxPartitionsPerGroup", 100)
  }

  def pushIoMaxRetries(conf: CssConf): Int = {
    conf.getInt("css.push.io.maxRetries", 3)
  }

  def pushIoRetryWaitMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.push.io.retryWait", "3s")
  }

  def pushQueueCapacity(conf: CssConf): Int = {
    conf.getInt("css.push.queue.capacity", 512)
  }

  def clientMapperEndTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.client.mapper.end.timeout", "600s")
  }

  def clientMapperEndSleepDeltaMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.client.mapper.end.sleep.delta", "50ms")
  }

  def pushDataRetryThreads(conf: CssConf): Int = {
    conf.getInt("css.pushData.retry.threads", 64)
  }

  def fetchChunkTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.fetch.chunk.timeout", "120s")
  }

  def fetchChunkMaxReqsInFlight(conf: CssConf): Int = {
    conf.getInt("css.fetch.chunk.maxReqsInFlight", 3)
  }

  def commitFilesParallelism(conf: CssConf): Int = {
    // parallelism from master to send commitFiles message to Workers
    conf.getInt("css.commitFiles.parallelism", 128)
  }

  def stageEndTimeoutMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.stage.end.timeout", "600s")
  }

  def stageEndRetryIntervalMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.stageEnd.retry.interval", "200ms")
  }

  def clientReallocateFailedMaxTimes(conf: CssConf): Int = {
    conf.getInt("css.client.reallocate.failed.max.times", 3)
  }

  def clientReallocateRetryIntervalMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.client.reallocate.retry.interval", "3s")
  }

  def localMode(conf: CssConf): Boolean = {
    conf.getBoolean("css.local.mode", false)
  }

  def masterAddress(conf: CssConf): String = {
    conf.get("css.master.address", "css://localhost:9099")
  }

  def clusterName(conf: CssConf): String = {
    conf.get("css.cluster.name", "default")
  }

  def haClusterName(conf: CssConf): String = {
    conf.get("css.ha.cluster.name", "")
  }

  def shuffleMode(conf: CssConf): ShuffleMode = {
    // DISK HDFS
    // scalastyle:off caselocale
    ShuffleMode.valueOf(conf.get("css.shuffle.mode", "DISK").toUpperCase)
    // scalastyle:on caselocale
  }

  def zkAddress(conf: CssConf): String = {
    conf.get("css.zookeeper.address", "")
  }

  def zkRetries(conf: CssConf): Int = {
    conf.getInt("css.zookeeper.retries", 5)
  }

  def workerRegistryType(conf: CssConf): String = {
    conf.get("css.worker.registry.type", "standalone")
  }

  def workerUpdateIntervalMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.worker.update.interval", "5s")
  }

  def extMetaKeepaliveIntervalMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.extMeta.keepalive.interval", "60s")
  }

  def extMetaExpireIntervalMs(conf: CssConf): Long = {
    conf.getTimeAsMs("css.extMeta.expire.interval", "480s")
  }

  def cleanerPeriodicGCInterval(conf: CssConf): Long = {
    conf.getTimeAsMs("css.cleaner.periodicGC.interval", "300s")
  }

  def backpressureEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.backpressure.enabled", true)
  }

  def backpressureLogEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.backpressure.log.enabled", false)
  }

  def backpressureMaxConcurrency(conf: CssConf): Int = {
    conf.getInt("css.backpressure.max.concurrency", 800)
  }

  def backpressureMinLimit(conf: CssConf): Int = {
    conf.getInt("css.backpressure.min.limit", 5)
  }

  def backpressureSmoothing(conf: CssConf): Double = {
    conf.getDouble("css.backpressure.smoothing", 0.2)
  }

  def backpressureRttTolerance(conf: CssConf): Double = {
    conf.getDouble("css.backpressure.rttTolerance", 1.5)
  }

  def backpressureLongWindow(conf: CssConf): Int = {
    conf.getInt("css.backpressure.longWindow", 10000)
  }

  def backpressureQueueSize(conf: CssConf): Int = {
    conf.getInt("css.backpressure.queueSize", 4)
  }

  def fixRateLimitThreshold(conf: CssConf): Int = {
    conf.getInt("css.fixRateLimit.threshold", 64)
  }

  def epochRotateThreshold(conf: CssConf): Long = {
    conf.getSizeAsBytes("css.epoch.rotate.threshold", "1g")
  }

  def zkSessionTimeoutMs(conf: CssConf): Int = {
    conf.getTimeAsMs("css.zk.timeout", "5min").toInt
  }

  def zkMaxParallelism(conf: CssConf): Int = {
    conf.getInt("css.zk.max.parallelism", 8)
  }

  def zkMetaExpiredLogEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.zk.metaExpired.log.enabled", true)
  }

  def localChunkFetchEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.local.chunk.fetch.enabled", true)
  }

  def minDiskBaseDirNum(conf: CssConf): Int = {
    conf.getInt("css.disk.dir.num.min", -1)
  }

  def failedBatchBlacklistEnable(conf: CssConf): Boolean = {
    /**
     * When MapTask encounters onFailure, if the feature is turned on.
     * the current reduceId-epochId-mapId-mapAttemptId-batchId will be recorded.
     * and ShuffleReader will explicitly ignore the data when reading.
     * because our deduplication is in Partition level deduplication.
     * but in the case of AE skewjoin.
     * the task read is likely to be an Epoch file. and the same Batch may appear in two Epochs.
     * In AE skewjoin mode, this switch must be turned on, otherwise there will be correctness problems.
     */
    conf.getBoolean("css.client.failed.batch.blacklist.enabled", true)
  }

  def testMode(conf: CssConf): Boolean = {
    conf.getBoolean("css.test.mode", false)
  }

  def compressionTestMode(conf: CssConf): Boolean = {
    conf.getBoolean("css.compression.test.mode", false)
  }

  def compressionCodecType(conf: CssConf): String = {
    // lz4, zstd
    conf.get("css.compression.codec", "lz4")
  }

  def zstdCompressionLevel(conf: CssConf): Int = {
    conf.getInt("css.zstd.compression.level", 3)
  }

  def zstdDictCompressionEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.zstd.compression.dict.enabled", false)
  }

  def sortPushSpillSizeThreshold(conf: CssConf): Long = {
    // hit 256m to spill
    conf.getSizeAsBytes("css.sortPush.spill.size.threshold", "256m")
  }

  def sortPushSpillRecordThreshold(conf: CssConf): Long = {
    // hit 1000000 record to spill
    conf.getLong("css.sortPush.spill.record.threshold", 1000000)
  }

  def chunkFetchRetryEnable(conf: CssConf): Boolean = {
    conf.getBoolean("css.chunk.fetch.retry.enabled", true)
  }

  def chunkFetchFailedRetryMaxTimes(conf: CssConf): Int = {
    conf.getInt("css.chunk.fetch.failed.retry.max.times", 3)
  }

  def chunkFetchRetryWaitTimes(conf: CssConf): Long = {
    conf.getTimeAsMs("css.chunk.fetch.retry.wait.times", "5s")
  }

  def maxAllocateWorker(conf: CssConf): Int = {
    conf.getInt("css.max.allocate.worker", 1000)
  }

  def workerAllocateExtraRatio(conf: CssConf): Double = {
    conf.getDouble("css.worker.allocate.extraRatio", 1.5)
  }

  def partitionAssignStrategy(conf: CssConf): String = {
    conf.get("css.partition.assign.strategy", "random")
  }
}
