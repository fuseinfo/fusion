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

import com.fuseinfo.fusion.spark.util.SparkUtils
import com.fuseinfo.fusion.util.VarUtils
import java.util
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

class CobolReader(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val optionSet = Set("copybook", "copybook_contents", "encoding", "ebcdic_code_page", "record_format",
    "record_length", "block_length", "records_per_block", "schema_retention_policy", "generate_record_id",
    "floating_point_format", "is_rdw_big_endian", "rdw_adjustment", "segment_field", "segment_filter")

  override def apply(vars:util.Map[String, String]): String = {
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val path = SparkUtils.stdPath(enrichedParams("path"))
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    logger.info("{} Reading mainframe file from {}", taskName, path:Any)
    val reader = spark.read.format("cobol")
    params.filter(p => optionSet.contains(p._1)).foreach(kv => reader.option(kv._1, kv._2.toString))
    val df = reader.load(path)
    SparkUtils.registerDataFrame(df, taskName, enrichedParams)
    s"Read COBOL files from $path lazily"
  }

  def getProcessorSchema:String = """{"title": "CobolReader","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.reader.CobolReader"},
    "path":{"type":"string","description":"Path of the COBOL files"},
    "copybook_contents":{"type":"string","format":"cobol","description":"COBOL Copybook",
                         "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "copybook":{"type":"string","description":"COBOL Copybook location"},
    "encoding":{"type":"string","format":"number","description":"default is ebcdic. can be ascii"},
    "ebcdic_code_page":{"type":"string","description":"code page"},
    "record_format":{"type":"string","description":"Record format of the file: F, V, FB, VB"},
    "record_length":{"type":"string","format":"number","description":"Record Length"},
    "block_length":{"type":"string","format":"number","description":"Block Length"},
    "records_per_block":{"type":"string","format":"number","description":"Records per Block"},
    "schema_retention_policy":{"type":"string","description":"collapse_root or keep_original"},
    "generate_record_id":{"type":"boolean","description":"Generate Record ID"},
    "floating_point_format":{"type":"string","format":"number","description":"Floating point format"},
    "is_rdw_big_endian":{"type":"boolean","format":"number","description":"is rdw big endian"},
    "rdw_adjustment":{"type":"string","format":"number","description":"RDW adjustment"},
    "segment_field":{"type":"string","format":"number","description":"segment field"},
    "segment_filter":{"type":"string","format":"number","description":"segment filter"},
    "repartition":{"type":"integer","description":"Number of partitions"},
    "cache":{"type":"string","description":"cache the DataFrame?"},
    "viewName":{"type":"string","description":"View Name to be registered"}
    },"required":["__class","path"]}"""
}
