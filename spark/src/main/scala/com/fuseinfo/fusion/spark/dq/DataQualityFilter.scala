package com.fuseinfo.fusion.spark.dq
import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class DataQualityFilter(taskName:String, params:java.util.Map[String, AnyRef])
  extends (java.util.Map[String, String] => String) with Serializable {
  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(vars: java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val table = enrichedParams.getOrElse("table", params("__previous"))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)

    val dq = params.map{case (key, details:java.util.Map[_, _]) =>
      val message = details.get("message") match {
        case msg: String => msg
        case _ => s"Rule $key violated."
      }
      val value = details.get("value")
      details.get("field") match {
        case field: String =>
          details.get("rule") match {
            case func: String =>
              if (value == null) Some(s"$func($field, '$message')")
              else Some(s"$func($field, '$message', $value)")
            case _ => None
          }
        case _ => None
      }
    case _ => None
    }.filter(_.nonEmpty).map(_.get).mkString(",")
    val df = spark.sql(s"SELECT *,array_remove(array($dq),'') _dq FROM $table")
    SparkUtils.registerDataFrame(df.filter("size(_dq) = 0").drop("_dq"), taskName, enrichedParams)
    enrichedParams.get("failed").foreach(failed =>
      SparkUtils.registerDataFrame(df.filter("size(_dq) > 0"), failed, enrichedParams))
    "Applied Data Quality Filter Successfully"
  }
}
