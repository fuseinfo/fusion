package com.fuseinfo.fusion.spark.reader

import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import java.util
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class JsonReader(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {
  def this(taskName:String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("path","repartition","cache","localCheckpoint","viewName")

  override def apply(vars:util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = SparkUtils.stdPath(enrichedParams("path"))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    logger.info("{} Reading Parquet file from {}", taskName, path:Any)
    val df = SparkUtils.getReader(spark, enrichedParams, optionSet).json(path)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read JSON files from $path lazily"
  }

  def getProcessorSchema:String = """{"title": "JsonReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.JsonReader"},
    "path":{"type":"string","description":"Path of the Parquet files"},
    "repartition":{"type":"integer","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache to memory"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","path"]}"""
}
