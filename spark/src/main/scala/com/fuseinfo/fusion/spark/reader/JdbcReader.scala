/*
 * Copyright (c) 2018 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.fuseinfo.fusion.spark.reader

import java.util.Properties

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class JdbcReader(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("driver","partitionColumn","lowerBound","upperBound","numPartitions")
  private val reservedSet = Set("url","table", "fetchsize", "where", "repartition", "cache")

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    logger.info("{} Reading database table from {}", taskName, enrichedParams("url"):Any)
    val reader = spark.read
    enrichedParams.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2))
    val props = new Properties
    enrichedParams.filterKeys{key => !optionSet.contains(key) && !reservedSet.contains(key) && !key.startsWith("__")}
        .foreach(kv => props.setProperty(kv._1, kv._2))
    val url = enrichedParams("url")
    val table = enrichedParams("table")
    val df = reader.jdbc(url, table, props)
    val df2 = enrichedParams.get("where") match {
      case Some(where) => df.where(where)
      case None => df
    }
    SparkUtils.registerDataFrame(df2, taskName, enrichedParams)
    s"Loaded $table from $url lazily"
  }

  override def getProcessorSchema:String = """{"title": "JdbcReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.JdbcReader"},
    "url":{"type":"string","description":"JDBC url of the database"},
    "driver":{"type":"string","description":"JDBC driver class of the database"},
    "table":{"type":"string","description":"The table or SQL query to be loaded"},
    "user":{"type":"string","description":"The JDBC user"},
    "password":{"type":"string","format":"password","description":"The password of the JDBC user"},
    "where":{"type":"string","description":"Add a WHERE clause to the table/SQL query"},
    "partitionColumn":{"type":"string","description":"Provide a numeric column/expression to partition of JDBC connection"},
    "lowerBound":{"type":"string","format":"number","description":"The lower Bound value of the partition column"},
    "upperBound":{"type":"string","format":"number","description":"The upper Bound value of the partition column"},
    "numPartitions":{"type":"string","format":"number","description":"The number of partitions"},
    "fetchsize":{"type":"string","format":"number","description":"Number of record to be fetch at a time"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","url","driver","table"]}"""
}
