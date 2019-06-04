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
package com.fuseinfo.fusion.spark.writer

import org.apache.spark.sql.{DataFrameWriter, Row, SparkSession}
import scala.collection.JavaConversions._

class CsvWriter(taskName:String, params:java.util.Map[String, AnyRef]) extends FileWriter(taskName, params) {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  private val reservedReader = Set("header","delimiter","quote","escape","nullValue","dateFormat")
  private val reservedWriter = reservedReader + "quoteMode"

  override def applyWriter(writer: DataFrameWriter[Row], path: String): Unit = {
    params.filter(p => reservedWriter.contains(p._1)).foreach{case (k, v) => writer.option(k, v.toString)}
    writer.csv(path)
  }

  override def countFile(spark: SparkSession, file: String): Long = {
    val reader = spark.read
    params.filter(p => reservedWriter.contains(p._1)).foreach{case (k, v) => reader.option(k, v.toString)}
    reader.csv(file).count
  }

  override def getProcessorSchema:String = """{"title": "CsvWriter","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.writer.CsvWriter"},
    "path":{"type":"string","description":"Path to save the output"},
    "table":{"type":"string","description":"Table name"},
    "header":{"type":"string","description":"header"},
    "delimiter":{"type":"string","description":"Delimiter"},
    "quote":{"type":"string","description":"Quote character"},
    "escape":{"type":"string","description":"Escape character"},
    "nullValue":{"type":"string","description":"null value"},
    "dateFormat":{"type":"string","description":"Date format"},
    "quoteMode":{"type":"string","description":"Quote mode"},
    "coalesce":{"type":"string","format":"number","description":"Number of partition to coalesce"},
    "partitionBy":{"type":"string","description":"Partition by"},
    "verifyCounts":{"type":"boolean","description":"Verify counts?"},
    "user":{"type":"string","description":"Run as a different user"},
    "keytab":{"type":"string","description":"keytab of the user"},
    "filePrefix":{"type":"string","description":"Prefix of the file name"},
    "staging":{"type":"string","description":"Staging location"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "onSuccess":{"type":"array","format":"tabs","description":"extension after success",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}},
    "onFailure":{"type":"array","format":"tabs","description":"extension after failure",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}}
    },"required":["__class","path"]}"""
}

