package com.fuseinfo.fusion.spark.reader

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util
import scala.collection.JavaConversions._

class EsReader(taskName:String, params: util.Map[String, AnyRef]) extends FusionFunction {
  def this(taskName: String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def init(params: util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars: util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val spark = SparkSession.getActiveSession.getOrElse((SparkSession.getDefaultSession.get))
    val reader = spark.read.format("org.elasticsearch.spark.sql")
    val index = enrichedParams("index")
    enrichedParams.foreach{ case (key, value) =>
      if (key.startsWith("es.")) reader.option(key, value)
    }
    val df = reader.load(index)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Loaded $index lazily"
  }

  override def getProcessorSchema:String = """{"title": "EsReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.EsReader"},
    "index":{"type":"string","description":"Elasticsearch index"},
    "es.nodes":{"type":"string","description":"Elasticsearch nodes"},
    "es.port":{"type":"string","description":"Elasticsearch port"},
    "es.net.ssl":{"type":"string","description":"Use SSL?"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"}
    },"required":["__class","index"]}"""
}