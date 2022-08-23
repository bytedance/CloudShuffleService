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

package com.bytedance.css.common.metrics

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import scala.collection.{mutable, Map}
import scala.collection.JavaConverters._
import scala.util.matching.Regex

import com.bytedance.css.common.CssConf
import com.bytedance.css.common.internal.Logging
import com.bytedance.css.common.util.Utils

class MetricsConfig(conf: CssConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "css-metrics.properties"

  val properties = new Properties()
  var perInstanceSubProperties: mutable.HashMap[String, Properties] = null

  def getDefaultMetricsFile(env: Map[String, String] = sys.env): Option[String] = {
    env.get("CSS_CONF_DIR")
      .orElse(env.get("CSS_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}${DEFAULT_METRICS_CONF_FILENAME}") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
  }

  /**
   * Load properties from various places, based on precedence
   * If the same property is set again latter on in the method, it overwrites the previous value
   */
  def initialize(): Unit = {

    val metricsFile = conf.getOption("css.metrics.conf")
      .orElse(getDefaultMetricsFile())

    loadPropertiesFromFile(metricsFile)

    // Also look for the properties in provided Css configuration
    val prefix = "css.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    // Now, let's populate a list of sub-properties per instance, instance being the prefix that
    // appears before the first dot in the property name.
    // Add to the sub-properties per instance, the default properties (those with prefix "*"), if
    // they don't have that exact same sub-property already defined.
    //
    // For example, if properties has ("*.class"->"default_class", "*.path"->"default_path,
    // "driver.path"->"driver_path"), for driver specific sub-properties, we'd like the output to be
    // ("driver"->Map("path"->"driver_path", "class"->"default_class")
    // Note how class got added to based on the default property, but path remained the same
    // since "driver.path" already existed and took precedence over "*.path"
    //
    perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
    if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  /**
   * Take a simple set of properties and a regex that the instance names (part before the first dot)
   * have to conform to. And, return a map of the first order prefix (before the first dot) to the
   * sub-properties under that prefix.
   *
   * For example, if the properties sent were Properties("*.sink.servlet.class"->"class1",
   * "*.sink.servlet.path"->"path1"), the returned map would be
   * Map("*" -> Properties("sink.servlet.class" -> "class1", "sink.servlet.path" -> "path1"))
   * Note in the subProperties (value of the returned Map), only the suffixes are used as property
   * keys.
   * If, in the passed properties, there is only one property with a given prefix, it is still
   * "unflattened". For example, if the input was Properties("*.sink.servlet.class" -> "class1"
   * the returned Map would contain one key-value pair
   * Map("*" -> Properties("sink.servlet.class" -> "class1"))
   * Any passed in properties, not complying with the regex are ignored.
   *
   * @param prop the flat list of properties to "unflatten" based on prefixes
   * @param regex the regex that the prefix has to comply with
   * @return an unflattened map, mapping prefix with sub-properties under that prefix
   */
  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1.toString).isDefined) {
        val regex(prefix, suffix) = kv._1.toString
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    perInstanceSubProperties.get(inst) match {
      case Some(s) => s
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

  /**
   * Loads configuration from a config file. If no config file is provided, try to get file
   * in class path.
   */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    logInfo(s"Using Css's metrics file: " +
      path.getOrElse(Utils.getContextOrClassLoader.getResource(DEFAULT_METRICS_CONF_FILENAME)))
    var is: InputStream = null
    try {
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getContextOrClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }
      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(s"Error loading configuration file $file", e)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

}
