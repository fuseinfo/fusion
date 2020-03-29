package com.fuseinfo.fusion.spark.writer

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class EsWriter(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))

    val index = enrichedParams("index")
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)

    val tableName = enrichedParams.getOrElse("table", params("__previous")).toString
    val df = spark.sqlContext.table(tableName)
    val df2 = enrichedParams.get("coalesce") match {
      case Some(num) => df.coalesce(num.toInt)
      case None =>
        enrichedParams.get("repartition") match {
          case Some(num) => df.repartition(num.toInt)
          case None => df
        }
    }

    val writer = df2.write.format("org.elasticsearch.spark.sql")
    enrichedParams.foreach{ case (key, value) =>
      if (key.startsWith("es.")) writer.option(key, value)
    }
    enrichedParams.get("mode").foreach(writer.mode)
    writer.save(index)
    s"Persisted data to elasticsearch index $index"
  }

  override def getProcessorSchema:String = """{"title": "EsWriter","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.writer.EsWriter"},
    "index":{"type":"string","description":"Elasticsearch index"},
    "sql":{"type":"string","description":"SQL query"},
    "table":{"type":"string","description":"Table name"},
    "es.nodes":{"type":"string","description":"Elasticsearch nodes"},
    "es.port":{"type":"string","description":"Elasticsearch port"},
    "es.net.ssl":{"type":"string","description":"Use SSL"},
    "coalesce":{"type":"string","format":"number","description":"Number of partitions to coalesce"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "onSuccess":{"type":"array","format":"tabs","description":"extension after success",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}},
    "onFailure":{"type":"array","format":"tabs","description":"extension after failure",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}}
    },"required":["__class","index"]}"""
}
