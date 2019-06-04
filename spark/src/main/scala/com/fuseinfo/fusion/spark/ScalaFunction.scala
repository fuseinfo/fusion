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
package com.fuseinfo.fusion.spark

import com.fuseinfo.common.ScalaCompiler
import com.fuseinfo.fusion.FusionFunction
import org.apache.spark.sql.{Dataset, Row}

class ScalaFunction(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val src = s"""import org.apache.spark.sql._
import org.apache.spark.sql.functions._
  new Function1[java.util.Map[String, String], Dataset[Row]]{
  override def apply(vars:java.util.Map[String, String]) = {
  val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
  import spark.implicits._
  ${params.get("scala").toString}
  }
}"""
    val func = new ScalaCompiler().compile[java.util.Map[String, String] => Dataset[Row]](src).newInstance()
    func(vars) match {
      case df: Dataset[_] => df.createOrReplaceTempView(taskName.toUpperCase)
      case _ =>
    }
    "Executed Scala Function"
  }

  override def getProcessorSchema:String = """{"title": "ScalaFunction","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.ScalaFunction"},
    "scala":{"type":"string","format":"scala","description":"Scala code",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}}
    },"required":["__class","scala"]}"""
}
