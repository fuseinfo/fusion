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
package com.fuseinfo.fusion.spark.util

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

object SparkUtils {
  def registerDataFrame(df: Dataset[Row], tableName:String, params:java.util.Map[String, AnyRef]): Unit = {
    val df2 = params.get("repartition") match {
      case num:String => df.repartition(num.toInt)
      case _ => df
    }
    val df3 = params.get("localCheckpoint") match {
      case bool: String => try (df2.localCheckpoint(bool.toBoolean)) catch {case e:Exception => df2}
      case _ => df2
    }
    params.get("cache") match {
      case "true" => df3.cache
      case str:String => try {df3.persist(StorageLevel.fromString(str))} catch {case e:Exception => df3.persist()}
      case _ =>
    }
    df3.createOrReplaceTempView(params.getOrDefault("viewName", tableName).toString.toUpperCase)
  }

  def stdPath(path: String): String = {
    if (path.length > 1 && path.charAt(0) == '\\') {
      path.replace("\\", "/")
    } else if (path.length > 3 && path.charAt(1) == ':' && path.charAt(2) == '\\') {
      val drive = path.charAt(0)
      if ((drive >= 'A' && drive <= 'Z' ) || (drive >= 'a' && drive <= 'z')) {
        path.replace("\\", "/")
      } else path
    } else path
  }
}
