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
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class ExcelReader (taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("useHeader", "dataAddress", "treatEmptyValuesAsNulls", "inferSchema",
    "addColorColumns", "timestampFormat", "maxRowsInMemory","excerptSize","workbookPassword")

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars: util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = SparkUtils.stdPath(enrichedParams("path"))
    logger.info("{} Reading Excel from {}", taskName, path:Any)
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val reader = spark.read.format("com.crealytics.spark.excel")
    params.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2.toString))

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val basePath = new Path(path)
    val fileStatus = fs.getFileStatus(basePath)
    val df = if (fileStatus.isDirectory) {
      fs.listStatus(basePath, new PathFilter(){
        override def accept(path: Path): Boolean = {
          val pathName = path.getName.toLowerCase()
          pathName.endsWith(".xls") || pathName.endsWith(".xlsx")
        }
      }).map(status => reader.load(status.getPath.toString)).reduce((df1, df2) => df1.union(df2))
    } else {
      reader.load(path)
    }
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read Excel files from $path lazily"
  }

  override def getProcessorSchema:String = """{"title": "ExcelReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.ExcelReader"},
    "path":{"type":"string","description":"Path of the excel files"},
    "useHeader":{"type":"boolean","description":"Has header?"},
    "dataAddress":{"type":"string","description":"Range of data, such as 'Sheet1'!A1:C4"},
    "treatEmptyValuesAsNulls":{"type":"boolean","description":"Treat empty values as nulls"},
    "inferSchema":{"type":"boolean","description":"Infer Schema?"},
    "addColorColumns":{"type":"boolean","description":"Add Color Columns?"},
    "timestampFormat":{"type":"string","description":"Timestamp format"},
    "maxRowsInMemory":{"type":"string","format":"number","description":"Max number of rows in memory"},
    "excerptSize":{"type":"string","format":"number","description":"Number of rows for inferring"},
    "workbookPassword":{"type":"string","description":"Workbook Password"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","path","useHeader"]}"""
}
