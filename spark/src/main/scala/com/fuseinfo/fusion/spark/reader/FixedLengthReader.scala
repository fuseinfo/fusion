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

import java.text.SimpleDateFormat

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.spark.util.{AvroUtils, SparkUtils}
import com.fuseinfo.fusion.util.VarUtils
import org.apache.avro.Schema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.util.Try

class FixedLengthReader(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars: java.util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = enrichedParams("path")
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val fields = params.get("fields").toString.split("\\,")
    var lastBgn = params.getOrElse("length", "-1").toString.toInt
    val columns = (for (i <- fields.length - 1 to 0 by -1) yield {
      val colon = fields(i).indexOf(':')
      val dash = fields(i).indexOf('-', colon + 1)
      val (begin, end) = if (dash < 0) (fields(i).substring(colon + 1).trim.toInt, lastBgn)
      else (fields(i).substring(colon + 1, dash).trim.toInt, fields(i).substring(dash + 1).trim.toInt)
      lastBgn = begin
      (fields(i).substring(0, colon), begin, end)
    }).reverse.toArray
    val schema =
      (params.get("schema") match {
        case avsc:String => Some(AvroUtils.toSqlType((new Schema.Parser).parse(avsc)).dataType)
        case _ => None
      }) match {
      case Some(dt) if dt.isInstanceOf[StructType] => dt.asInstanceOf[StructType]
      case _ => StructType(columns.map(column => StructField(column._1, DataTypes.StringType)))
    }
    val columnMap = columns.map(field => field._1.toLowerCase -> (field._2, field._3)).toMap
    val fieldArray = schema.fields.map{field =>
      (field.name, columnMap.getOrElse(field.name.toLowerCase, (-1, -1)), field.dataType)
    }
    logger.info("{} Reading fixed length file from {}", taskName, path:Any)
    val rdd = spark.sparkContext.textFile(path).mapPartitions{iter =>
      val sdf = params.get("dateFormat") match {
        case s:String =>  new SimpleDateFormat(s)
        case _ => null}
      val tf = params.get("timestampFormat") match {
        case s:String => new SimpleDateFormat(s)
        case _ => null
      }
      iter.map{text =>
        val values = fieldArray.map{field =>
          val value = if (field._2._1 < 0) null
          else if (field._2._2 < 0) text.substring(field._2._1).trim else text.substring(field._2._1, field._2._2).trim
          field._3 match {
            case BooleanType => Try(value.toBoolean).getOrElse(null)
            case DateType => Try{if (sdf == null) java.sql.Date.valueOf(value)
                               else new java.sql.Date(sdf.parse(value).getTime)}.getOrElse(null)
            case DoubleType => Try(value.toDouble).getOrElse(null)
            case IntegerType => Try(value.toInt).getOrElse(null)
            case LongType => Try(value.toLong).getOrElse(null)
            case TimestampType => Try{if (tf == null) java.sql.Timestamp.valueOf(value)
                               else new java.sql.Timestamp(tf.parse(value).getTime)}.getOrElse(null)
            case decimal:DecimalType => Try(new java.math.BigDecimal(value)).getOrElse(null)
            case s:StringType => Try(value).getOrElse(null)
            case _ => value
          }
        }
        Row(values:_*)
      }
    }
    val df = spark.createDataFrame(rdd, schema)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read fixed length files from $path lazily"
  }

  override def getProcessorSchema:String = """{"title": "FixedLengthReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.FixedLengthReader"},
    "path":{"type":"string","description":"Path of the fix length files"},
    "fields":{"type":"string","description":"List of fields"},
    "schema":{"type":"string","description":"Schema of the fix length files"},
    "dateFormat":{"type":"string","description":"Date format"},
    "timestampFormat":{"type":"string","description":"Timestamp format"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"boolean","description":"cache the DataFrame?"}
    },"required":["__class","path","fields"]}"""
}
