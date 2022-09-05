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

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import com.fuseinfo.fusion.spark.util.{AvroUtils, SparkUtils}
import com.fuseinfo.fusion.util.VarUtils
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader}
import java.util.regex.Pattern
import scala.collection.JavaConversions._

class CsvReader(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  private val reserved = Set("path","schema","fields", "skipLines")

  override def apply(vars:util.Map[String, String]): String = {
    val columnNameOfCorruptRecord = params.getOrDefault("columnNameOfCorruptRecord", "_corrupt_record").toString
    val schemaOption = params.get("schema") match {
      case avsc:String => AvroUtils.toSqlType((new Schema.Parser).parse(avsc)).dataType match {
        case dt:StructType =>
          dt.add(StructField(columnNameOfCorruptRecord, StringType, true))
          Some(dt)
        case _ => None
      }
      case _ =>
        params.get("fields") match {
          case fields:String => Some(new StructType(fields.split(",").map(field =>
            StructField(field, DataTypes.StringType, true))))
          case _ => None
        }
    }

    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = SparkUtils.stdPath(enrichedParams("path"))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)

    val headerFlag = enrichedParams.getOrElse("header", "false").toBoolean
    // workaround for SPARK-21289
    val confOption = params.get("charset") match {
      case charset:String if charset.startsWith("UTF-16") || charset.startsWith("UTF-32") =>
        val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
        val newline = new String(new String("\n").getBytes(charset), "UTF-8")
        conf.set("textinputformat.record.delimiter", newline)
        Some(conf)
      case _ =>
        enrichedParams.get("skipLines") match {
          case None => None
          case Some("0") => None
          case Some(skipLines) => Some(new Configuration(spark.sparkContext.hadoopConfiguration))
        }
    }
    val df = confOption match {
      case Some(conf) =>
        logger.info("{} Reading CSV file from {} using text mode", taskName, path:Any)
        parsingToDataFrame(spark, conf, path, schemaOption, columnNameOfCorruptRecord, headerFlag,
          enrichedParams.getOrElse("skipLines", "0").toInt)
      case None =>
        logger.info("{} Reading CSV file from {} using Spark csv data source", taskName, path:Any)
        val reader = spark.read
        schemaOption match {
          case Some(aSchema) => reader.schema(aSchema)
          case None =>
        }
        enrichedParams.filter(p => !reserved.contains(p._1) && !p._1.startsWith("__"))
          .foreach(kv => reader.option(kv._1, kv._2))
        val rowDf = reader.csv(path)
        val schema = rowDf.schema.add(columnNameOfCorruptRecord, StringType)
        reader.schema(schema).csv(path)
    }
    val dfGood = {
      if (df.schema.fieldNames.contains(columnNameOfCorruptRecord)) {
        val dfBad = df.cache.filter(col(columnNameOfCorruptRecord).isNotNull).select(columnNameOfCorruptRecord)
        dfBad.createOrReplaceTempView(taskName + "_CORRUPT_RECORD")
        dfBad.createOrReplaceTempView(taskName + "_CORRUPT_RECORD")
        df.filter(col(columnNameOfCorruptRecord).isNull).drop(columnNameOfCorruptRecord)
      } else df

    }
    SparkUtils.registerDataFrame(dfGood, taskName, enrichedParams)
    s"Read CSV files from $path lazily"
  }

  private def createParser() = {
    val settings = new CsvParserSettings
    val format = settings.getFormat
    params.get("delimiter") match {
      case s: String if s.length > 0 => format.setDelimiter(s.charAt(0))
      case _ =>
    }
    params.get("quote") match {
      case s: String if s.length > 0 => format.setQuote(s.charAt(0))
      case _ =>
    }
    params.get("escape") match {
      case s: String if s.length > 0 => format.setQuoteEscape(s.charAt(0))
      case _ =>
    }
    params.get("comment") match {
      case s: String if s.length > 0 => format.setComment(s.charAt(0))
      case _ =>
    }
    params.get("charToEscapeQuoteEscaping") match {
      case s: String if s.length > 0 => format.setCharToEscapeQuoteEscaping(s.charAt(0))
      case _ =>
    }
    params.get("ignoreLeadingWhiteSpace") match {
      case s: String => settings.setIgnoreLeadingWhitespaces(java.lang.Boolean.parseBoolean(s))
      case _ =>
    }
    params.get("ignoreTrailingWhiteSpace") match {
      case s: String => settings.setIgnoreTrailingWhitespaces(java.lang.Boolean.parseBoolean(s))
      case _ =>
    }
    params.get("maxColumns") match {
      case s: String => settings.setMaxColumns(s.toInt)
      case _ =>
    }
    params.get("maxCharsPerColumn") match {
      case s: String => settings.setMaxCharsPerColumn(s.toInt)
      case _ =>
    }
    new CsvParser(settings)
  }

  private def parsingToDataFrame(spark:SparkSession, conf:Configuration, path:String, schemaOption:Option[StructType],
                                 columnNameOfCorruptRecord:String, headerFlag:Boolean, skip:Int) = {
    val parser = createParser()
    val skipLines = if (headerFlag) skip + 1 else skip
    val ts = System.currentTimeMillis()
    val paths = if (path.indexOf('*') > 0) {
      val slash = path.lastIndexOf("/")
      val dir = new Path(path.substring(0, slash))
      val fs = FileSystem.get(dir.toUri, conf)
      val regex = Pattern.compile(path.substring(slash + 1).replaceAll("\\*", ".*"))
      fs.listStatus(dir, new PathFilter() {
        override def accept(path: Path): Boolean = {
          regex.matcher(path.getName).matches()
        }
      }).map(_.getPath.toString).mkString(",")
    } else path

    val schema = if (schemaOption.isEmpty) {
      val path = new Path(paths.split(",", 2).head)
      val fs = FileSystem.get(path.toUri, conf)
      val f = if (fs.isDirectory(path)) {
        fs.listStatus(path, new PathFilter() {
          override def accept(path: Path): Boolean = !path.getName.startsWith("_") && !path.getName.startsWith(".")
        }).head.getPath
      } else path
      val br = new BufferedReader(new InputStreamReader(fs.open(f)))
      0 until skip foreach(_ => br.readLine())
      val line = br.readLine()
      scala.util.Try(br.close())
      val columns = parser.parseLine(line) :+ columnNameOfCorruptRecord
      if (headerFlag) new StructType(columns.map(field => StructField(field, DataTypes.StringType, true)))
      else StructType((columns.indices.map(i => "c" + i) :+ columnNameOfCorruptRecord)
        .map(c => StructField(c, DataTypes.StringType, true)).toArray)
    } else schemaOption.get

    val rdd = spark.sparkContext
      .newAPIHadoopFile(paths, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .mapPartitionsWithIndex{case (idx, iter) => if (idx == 0) iter.drop(skipLines) else iter}
      .mapPartitions{iter =>
        val parser = createParser()
        val charset = params.getOrDefault("charset","UTF-8").toString
        val fields = schema.fields
        val len = fields.length - 1
        val df = params.get("dateFormat") match {
          case s:String => new SimpleDateFormat(s)
          case _ => null
        }
        val tf = params.get("timestampFormat") match {
          case s:String => new SimpleDateFormat(s)
          case _ => null
        }
        iter.map{pair =>
          val line = new String(pair._2.getBytes, 0, pair._2.getLength, charset)
          try {
            val list = parser.parseLine(line)
            val data = fields.zipAll(if (list.length > len) list.slice(0, len) else list, null, null)
              .map{
                case (_, null) => null
                case (structField, "") if structField.dataType == StringType => ""
                case (_, "") => null
                case (structField, value) => structField.dataType match {
                  case BooleanType => value.toBoolean
                  case DateType =>
                    if (df == null) java.sql.Date.valueOf(value) else new java.sql.Date(df.parse(value).getTime)
                  case DoubleType => value.toDouble
                  case IntegerType => value.toInt
                  case LongType => value.toLong
                  case TimestampType =>
                    if (tf == null) Timestamp.valueOf(value) else new Timestamp(tf.parse(value).getTime)
                  case decimal:DecimalType => new java.math.BigDecimal(value)
                  case _ => value
                }
              }
            Row(data: _*)
          } catch {
            case e:Exception =>
              val data = new Array[AnyRef](len + 1)
              data(len) = line
              Row(data: _*)
          }

        }
      }
    spark.createDataFrame(rdd, schema)
  }

  def getProcessorSchema:String = """{"title": "CsvReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.CsvReader"},
    "path":{"type":"string","description":"Path of the CSV files"},
    "schema":{"type":"string","description":"Schema of the CSV files"},
    "fields":{"type":"string","description":"List of fields"},
    "header":{"type":"boolean","description":"has Header?"},
    "charset":{"type":"string","description":"Charset of the CSV files"},
    "delimiter":{"type":"string","description":"Delimiter character of the CSV files"},
    "quote":{"type":"string","description":"Quote character of the CSV files"},
    "escape":{"type":"string","description":"Escape character of the CSV files"},
    "skipLines":{"type":"string","format":"number","description":"Number of lines to skip"},
    "nullValue":{"type":"string","description":"null value"},
    "multiLine":{"type":"boolean","description":"Support multiline?"},
    "inferSchema":{"type":"boolean","description":"Infer Schema?"},
    "dateFormat":{"type":"string","description":"Date Format"},
    "timeZone":{"type":"string","description":"Time zone"},
    "timestampFormat":{"type":"string","description":"Timestamp format"},
    "maxColumns":{"type":"string","format":"number","description":"Max number of columns"},
    "maxCharsPerColumn":{"type":"string","format":"number","description":"Max chars per column"},
    "comment":{"type":"string","description":"Comment character"},
    "charToEscapeQuoteEscaping":{"type":"string","description":"Char to escape quote escaping"},
    "ignoreLeadingWhiteSpace":{"type":"boolean","description":"Ignore leading white space"},
    "ignoreTrailingWhiteSpace":{"type":"boolean","description":"Ignore trailing white space"},
    "columnNameOfCorruptRecord":{"type":"string","description":"Column name of corrupt record"},
    "repartition":{"type":"string","format":"number","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"}
    },"required":["__class","path"]}"""
}
