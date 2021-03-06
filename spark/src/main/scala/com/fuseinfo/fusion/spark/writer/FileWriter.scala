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
package com.fuseinfo.fusion.spark.writer

import java.security.PrivilegedAction
import java.util

import com.fuseinfo.fusion.util.{ClassUtils, VarUtils}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

abstract class FileWriter(taskName:String, params:util.Map[String, AnyRef])
  extends (util.Map[String, String] => String) with Serializable {

  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  @transient lazy val extensions: Map[String, Array[util.Map[String, String]]] = params
    .filter(_._2.isInstanceOf[Array[_]]).toMap.asInstanceOf[Map[String, Array[util.Map[String, String]]]]

  def apply(vars: util.Map[String, String]): String = {
    val successExts = extensions.getOrElse("onSuccess", Array.empty).map{props =>
      try {
        ClassUtils.newExtension(props("__class"), props.toMap.filter(_._1 != "__class"))
      } catch {
        case e:Exception =>
          logger.warn("{} Unable to create an onSuccess extension", taskName, e:Any)
          null
      }
    }.filter(_ != null)
    val failureExts = extensions.getOrElse("onFailure", Array.empty).map{props =>
      try {
        ClassUtils.newExtension(props("__class"), props.toMap.filter(_._1 != "__class"))
      } catch {
        case e:Exception =>
          logger.warn("{} Unable to create an onFailure extension", taskName, e:Any)
          null
      }
    }.filter(_ != null)
    try {
      val enrichedParams = params.filter(_._2.isInstanceOf[String])
        .mapValues(v => VarUtils.enrichString(v.toString, vars))
      val stagingBase = enrichedParams.getOrElse("staging", ".fusionStaging")
      val verifyCounts = enrichedParams.getOrElse("verifyCounts", "false").toBoolean

      val path = enrichedParams("path")
      val filePrefix = enrichedParams.getOrElse("filePrefix", taskName)
      val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)

      val conf = spark.sparkContext.hadoopConfiguration
      val fs = (enrichedParams.get("user"), enrichedParams.get("keytab")) match {
        case (Some(user), Some(keytab)) =>
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
            .doAs(new PrivilegedAction[FileSystem]() {
              override def run(): FileSystem = FileSystem.get(conf)
            })
        case _ => FileSystem.get(conf)
      }

      val uuid = util.UUID.randomUUID.toString
      val stagingLoc = stagingBase + "/" + uuid
      val df = enrichedParams.get("sql") match {
        case Some(sqlText) => spark.sql(sqlText)
        case None => getDataFrame(spark, enrichedParams.getOrElse("table", params("__previous")).toString)
      }

      val writer = enrichedParams.get("partitionBy") match {
        case Some(partitionBy) =>
          val partitionCols = partitionBy.split(",").map(_.trim)
          df.repartition(partitionCols.map(c => col(c)).toSeq: _*).write.partitionBy(partitionCols.toSeq: _*)
        case None => (enrichedParams.get("coalesce") match {
          case Some(num) => df.coalesce(num.toInt)
          case None =>
            enrichedParams.get("repartition") match {
              case Some(num) => df.repartition(num.toInt)
              case None => df
            }
        }).write
      }
      applyWriter(writer, stagingLoc)
      val dfCount = if (verifyCounts) df.count else -1

      val stagingPath = new Path(stagingLoc)
      val iter = fs.listFiles(stagingPath, true)
      val files = collection.mutable.ArrayBuffer.empty[LocatedFileStatus]
      while (iter.hasNext) {
        val lfs = iter.next
        if (lfs.getPath.getName.startsWith("part-")) files.append(lfs)
      }
      val basePath = new Path(path)
      scala.util.Try(fs.mkdirs(basePath))
      //val baseDir = if (basePath.isAbsoluteAndSchemeAuthorityNull) basePath.toString
      //else fs.getFileStatus(basePath).getPath.toUri.getPath

      val time = System.currentTimeMillis.toString
      fs.deleteOnExit(stagingPath)
      val destPath = new Path(path)

      enrichedParams.get("backup") match {
        case Some(backup) =>
          if (files.nonEmpty && fs.isDirectory(destPath)) {
            val backupFiles = fs.listStatus(destPath).filter(_.getPath.getName.charAt(0) != '.')
            if (backupFiles.nonEmpty) {
              val backupPath = new Path(backup)
              scala.util.Try(fs.mkdirs(backupPath))
              backupFiles.foreach(file => fs.rename(file.getPath, new Path(backupPath, file.getPath.getName)))
            }
          }
        case None =>
      }

      val statsWithPath = files.map { file =>
        val filePath = file.getPath
        val srcParent = filePath.getParent.toString
        val destDir = path + "/" + srcParent.substring(srcParent.indexOf(uuid) + uuid.length)
        val destDirPath = new Path(destDir)
        val fileName = filePath.getName
        val destName = filePrefix + time + fileName.substring(4, 12) + fileName.substring(fileName.indexOf('.'))
        val destFilePath = new Path(destDirPath, destName)
        val filePartName = destFilePath.toString
        val rowCount = if (dfCount >= 0) countFile(spark, filePath.toString) else {
          scala.util.Try(fs.mkdirs(destDirPath))
          fs.rename(filePath, destFilePath)
          logger.info("{} Persisted file {}", taskName, filePartName:Any)
          -1
        }
        (filePath, destFilePath, Map("path" -> filePartName) ++
          (if (rowCount < 0) Map.empty[String, String] else Map("rowCount" -> rowCount.toString)))
      }
      val stats = enrichedParams ++ statsWithPath.zipWithIndex.flatMap{stats =>
        val idx = "." + stats._2
        stats._1._3.map(kv => (kv._1 + idx) -> kv._2)
      }

      val allStats = stats + ("rowCount" -> dfCount.toString)
      if (dfCount > 0) {
        val fileCount = statsWithPath.map(_._3.getOrElse("rowCount", "0").toLong).sum
        if (fileCount == dfCount) {
          statsWithPath.foreach { t =>
            scala.util.Try(fs.mkdirs(t._2.getParent))
            fs.rename(t._1, t._2)
            logger.info("{} Persisted file {} with {} record(s)", taskName, t._2.toString, t._3.getOrElse("rowCount", "0"))
          }
          successExts.foreach(ext => scala.util.Try(ext(allStats)))
          s"Persisted file with $dfCount records"
        } else {
          failureExts.foreach(ext => scala.util.Try(ext(stats + ("error" -> "Record count mismatched"))))
          throw new RuntimeException(taskName + ": Record count mismatched")
        }
      } else {
        successExts.foreach(ext => scala.util.Try(ext(allStats)))
        if (dfCount == 0) "Completed with zero record" else "Persisted file without verification"
      }
    } catch {
      case e: Throwable =>
        failureExts.foreach(ext => scala.util.Try(ext(Map("error" -> e.getMessage))))
        throw new RuntimeException(taskName + ": Failed to persist output", e)
    }
  }

  def getDataFrame(spark:SparkSession, tableName:String):Dataset[Row] = spark.sqlContext.table(tableName)

  def applyWriter(writer:DataFrameWriter[Row], path:String):Unit

  def countFile(spark:SparkSession, file:String):Long

}
