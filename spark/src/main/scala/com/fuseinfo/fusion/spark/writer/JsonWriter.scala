package com.fuseinfo.fusion.spark.writer

import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SparkSession}

class JsonWriter(taskName:String, params:java.util.Map[String, AnyRef]) extends FileWriter(taskName, params) {
  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  override def getDataFrame(spark:SparkSession, tableName:String): Dataset[Row] = {
    val df = super.getDataFrame(spark, tableName)
    df.schema.fields.foldLeft(df){(dfNew, field) => if (field.name.exists(c => " ,;{}()\n\t=".indexOf(c) > 0))
      dfNew.withColumnRenamed(field.name, field.name.replace(' ', '_')
        .replaceAll("\\s|,|;|\\{|\\}|\\(|\\)|=",""))
    else dfNew
    }
  }

  override def applyWriter(writer: DataFrameWriter[Row], path: String): Unit = writer.json(path)

  override def countFile(spark: SparkSession, file: String): Long = spark.read.json(file).count

  def getProcessorSchema:String = """{"title": "JsonWriter","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.writer.JsonWriter"},
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
