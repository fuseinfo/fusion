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

import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SparkSession}

class AvroWriter(taskName:String, params:java.util.Map[String, AnyRef]) extends FileWriter(taskName, params) {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  override def getDataFrame(spark:SparkSession, tableName:String): Dataset[Row] = {
    val df = super.getDataFrame(spark, tableName)
    df.schema.fields.foldLeft(df){(dfNew, field) => if (field.name.exists(c => " ,;{}()\n\t=".indexOf(c) > 0))
      dfNew.withColumnRenamed(field.name, field.name.replace(' ', '_')
        .replaceAll("\\s|,|;|\\{|\\}|\\(|\\)|=",""))
      else dfNew
    }
  }

  override def applyWriter(writer: DataFrameWriter[Row], path: String): Unit = writer.format("avro").save(path)

  override def countFile(spark: SparkSession, file: String): Long = spark.read.format("avro").load(file).count

  def getProcessorSchema:String = """{"title": "AvroWriter","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.writer.AvroWriter"},
    "path":{"type":"string","description":"Path to save the output"},
    "sql":{"type":"string","format":"sql","description":"Spark SQL statement",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "table":{"type":"string","description":"Table name"},
    "mode":{"type":"string","description":"Save mode"},
    "partitionBy":{"type":"string","description":"Partition by"},
    "verifyCounts":{"type":"boolean","description":"Verify counts?"},
    "user":{"type":"string","description":"Run as a different user"},
    "keytab":{"type":"string","description":"keytab of the user"},
    "filePrefix":{"type":"string","description":"Prefix of the file name"},
    "staging":{"type":"string","description":"Staging location"},
    "coalesce":{"type":"string","format":"number","description":"Number of partitions to coalesce"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "onSuccess":{"type":"array","format":"tabs","description":"extension after success",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}},
    "onFailure":{"type":"array","format":"tabs","description":"extension after failure",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}}
    },"required":["__class","path"]}"""
}
