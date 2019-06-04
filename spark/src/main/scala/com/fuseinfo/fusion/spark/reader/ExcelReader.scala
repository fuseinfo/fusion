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

import java.util

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class ExcelReader (taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("sheet", "dateFormat", "timestampFormat", "emptyValue", "inferSchema", "header",
    "skipLines","fields")

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars: util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = enrichedParams("path")
    logger.info("{} Reading Excel from {}", taskName, path:Any)
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val reader = spark.read.format("com.fuseinfo.spark.sql.sources.v2.excel")
    params.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2.toString))
    val df = reader.load(path)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read Excel files from $path lazily"
  }

  override def getProcessorSchema:String = """{"title": "ExcelReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.ExcelReader"},
    "path":{"type":"string","description":"Path of the excel files"},
    "sheet":{"type":"string","description":"Sheet name of the excel files"},
    "header":{"type":"string","description":"Header line of the excel files"},
    "fields":{"type":"string","description":"List of fields"},
    "dateFormat":{"type":"string","description":"Date format"},
    "timestampFormat":{"type":"string","description":"Timestamp format"},
    "skipLines":{"type":"string","format":"number","description":"Number of lines to skip"},
    "emptyValue":{"type":"string","description":"Empty value of the excel files"},
    "inferSchema":{"type":"boolean","description":"Infer Schema?"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"boolean","description":"cache the DataFrame?"}
    },"required":["__class","path"]}"""
}
