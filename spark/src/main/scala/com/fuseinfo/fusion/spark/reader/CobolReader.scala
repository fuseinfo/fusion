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

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class CobolReader(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("copybook", "binaryformat", "bookname", "copybookformat", "emptyvalue", "font",
    "nullvalue","number","recordformat","split","tree")

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = enrichedParams("path")
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    logger.info("{} Reading mainframe file from {}", taskName, path:Any)
    val reader = spark.read.format("com.fuseinfo.spark.sql.sources.v2.cobol")
    params.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2.toString))
    val df = reader.load(path)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read COBOL files from $path lazily"
  }

  override def getProcessorSchema:String = """{"title": "CobolReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.CobolReader"},
    "path":{"type":"string","description":"Path of the COBOL files"},
    "copybook":{"type":"string","description":"COBOL Copybook"},
    "bookname":{"type":"string","description":"COBOL Copybook"},
    "font":{"type":"string","description":"COBOL Copybook"},
    "recordformat":{"type":"string","description":"Record format of the file: F, V, FB, VB"},
    "copybookformat":{"type":"string","format":"number","description":"COBOL Copybook format:"},
    "binaryformat":{"type":"string","format":"number","description":"COBOL Copybook format:"},
    "tree":{"type":"string","format":"number","description":"COBOL Copybook format:"},
    "nullvalue":{"type":"string","description":"COBOL Copybook"},
    "emptyvalue":{"type":"string","description":"COBOL Copybook"},
    "number":{"type":"string","format":"number","description":"COBOL Copybook format:"},
    "repartition":{"type":"integer","description":"Number of partitions"},
    "cache":{"type":"boolean","description":"cache the DataFrame?"}
    },"required":["__class","path","copybook"]}"""
}
