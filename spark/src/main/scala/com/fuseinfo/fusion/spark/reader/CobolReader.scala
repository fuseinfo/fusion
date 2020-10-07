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

import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import java.util
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class CobolReader(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("copybook", "binaryformat", "bookname", "copybookformat", "emptyvalue", "font",
    "nullvalue","number","recordformat","split","tree")

  override def apply(vars:util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = SparkUtils.stdPath(enrichedParams("path"))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    logger.info("{} Reading mainframe file from {}", taskName, path:Any)
    val reader = spark.read.format("com.fuseinfo.spark.sql.sources.v2.cobol")
    params.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2.toString))
    val df = reader.load(path)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read COBOL files from $path lazily"
  }

  def getProcessorSchema:String = """{"title": "CobolReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.CobolReader"},
    "path":{"type":"string","description":"Path of the COBOL files"},
    "copybook":{"type":"string","description":"COBOL Copybook"},
    "bookname":{"type":"string","description":"The common prefix of field names to be ignored"},
    "recordformat":{"type":"string","description":"Record format of the file: F, V, FB, VB"},
    "binaryformat":{"type":"string","format":"number","description":"Binary format: 0 - Intel; 2 - Mainframe"},
    "font":{"type":"string","description":"code page"},
    "copybookformat":{"type":"string","format":"number","description":"COBOL Copybook format"},
    "emptyvalue":{"type":"string","description":"use this string when empty is found},
    "nullvalue":{"type":"string","description":"use this string when null is found"},
    "number":{"type":"string","format":"number","description":"How to cast numbers: decimal, double, string, mixed"},
    "tree":{"type":"string","format":"number","description":"Whether use tree/nested format"},
    "repartition":{"type":"integer","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","path","copybook"]}"""
}
