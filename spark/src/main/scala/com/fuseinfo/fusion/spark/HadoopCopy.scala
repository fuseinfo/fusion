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

import java.net.URI
import java.security.{DigestOutputStream, MessageDigest, PrivilegedAction}
import java.util.UUID
import java.util.regex.{Matcher, Pattern}

import com.fuseinfo.fusion.FusionFunction
import com.fuseinfo.fusion.util.{ClassUtils, VarUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Try

class HadoopCopy(taskName:String, params:java.util.Map[String, AnyRef]) extends FusionFunction {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  @transient private lazy val extensions: Map[String, Array[java.util.Map[String, String]]] = params.filter(_._2.isInstanceOf[Array[_]])
    .toMap.asInstanceOf[Map[String, Array[java.util.Map[String, String]]]]

  case class CopyTask(timestamp:Long, size:Long, fileStatus:FileStatus, matcher:Matcher)

  @transient private lazy val postCopyFunc = params.get("afterCopy") match {
    case func:String =>
      Some(Class.forName(func).newInstance.asInstanceOf[(FileSystem, Path, Option[Array[Byte]], AnyRef) => Boolean])
    case _ => None
  }

  private val processedSet = new java.util.concurrent.ConcurrentHashMap[Path, Unit]
  //private val failedSet = new java.util.concurrent.ConcurrentHashMap[Path, Unit]

  override def init(params: java.util.Map[String, AnyRef]): Unit = {
    this.params.clear()
    this.params.putAll(params)
  }

  override def apply(vars:java.util.Map[String, String]): String = {
    val successExts = extensions.getOrElse("onSuccess", Array.empty).map{props =>
      try {
        ClassUtils.newExtension(props("__class"), props.toMap.filter(_._1 != "__class")
          .mapValues(VarUtils.enrichString(_, vars)))
      } catch {
        case e:Exception =>
          logger.warn("Unable to create an onSuccess extension", e)
          null
      }
    }.filter(_ != null)
    val failureExts = extensions.getOrElse("onFailure", Array.empty).map{props =>
      try {
        ClassUtils.newExtension(props("__class"), props.toMap.filter(_._1 != "__class")
          .mapValues(VarUtils.enrichString(_, vars)))
      } catch {
        case e:Exception =>
          logger.warn("Unable to create an onFailure extension", e)
          null
      }
    }.filter(_ != null)
    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    val stagingPath = new Path(enrichedParams.getOrElse("staging", ".fusionStaging") + "/" + UUID.randomUUID.toString)
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
    val conf = spark.sparkContext.hadoopConfiguration
    val source = enrichedParams("source")
    val sourceUri = new URI(source)
    val sourceFs = (enrichedParams.get("sourceUser"), enrichedParams.get("sourceKeytab")) match {
      case (Some(user), Some(keytab)) =>
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
          .doAs(new PrivilegedAction[FileSystem](){
            override def run(): FileSystem = FileSystem.get(sourceUri, conf)})
      case _ => FileSystem.get(sourceUri, conf)
    }
    val target = enrichedParams("target")
    val targetUri = new URI(target)
    val targetFs = (enrichedParams.get("targetUser"), enrichedParams.get("targetKeytab")) match {
      case (Some(user), Some(keytab)) =>
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab)
          .doAs(new PrivilegedAction[FileSystem](){
            override def run(): FileSystem = FileSystem.get(targetUri, conf)})
      case _ => FileSystem.get(targetUri, conf)
    }
    Try(targetFs.mkdirs(stagingPath))
    targetFs.deleteOnExit(stagingPath)
    val md = enrichedParams.get("digest").map(alg => MessageDigest.getInstance(alg))
    val codec = enrichedParams.get("compressCodec").map(Class.forName(_).newInstance.asInstanceOf[CompressionCodec])
    val pattern = Pattern.compile(params.getOrDefault("pattern", ".+").toString)
    val recursive = enrichedParams.getOrElse("recursive", "false").toBoolean
    val interval = enrichedParams.getOrElse("interval", "60000").toLong
    val wait = enrichedParams.getOrElse("wait", "60000").toLong
    val preCopyFunc = params.get("beforeCopy") match {
      case func: String => Some(Class.forName(func).newInstance.asInstanceOf[(FileSystem, Path) => (Path, AnyRef)])
      case _ => None
    }

    val queue = collection.mutable.Queue.empty[CopyTask]
    while (true) {
      val startTime = System.currentTimeMillis
      try {
        val riter = sourceFs.listFiles(new Path(sourceUri), recursive)
        while (riter.hasNext) {
          val lfs = riter.next
          val path = lfs.getPath
          val matcher = pattern.matcher(path.getName)
          if (matcher.matches() && !processedSet.containsKey(path)) {
            logger.info("{}: Found file: {}", taskName, lfs.getPath.toString:Any)
            processedSet.put(lfs.getPath, Unit)
            queue += CopyTask(System.currentTimeMillis, 0L, lfs, matcher)
          }
        }

        while (queue.nonEmpty && queue.head.timestamp <= System.currentTimeMillis) {
          val ct = queue.dequeue
          preCopyFunc match {
            case Some(func) =>
              Try {
                val ctFilePath = ct.fileStatus.getPath
                val output = func(sourceFs, ctFilePath)
                if (output != null) doCopy(conf, sourceFs, output._1, targetFs, target, stagingPath,
                  ct.matcher, md, codec, successExts, failureExts, output._2)
                else {
                  logger.error("{}: beforeCopy {} for file {}", taskName, func.getClass.getName, ctFilePath.toString)
                }
              }
            case None =>
              if (ct.size == 0 || ct.size != sourceFs.getFileStatus(ct.fileStatus.getPath).getLen) {
                queue += CopyTask(System.currentTimeMillis + wait, ct.fileStatus.getLen, ct.fileStatus, ct.matcher)
              } else doCopy(conf, sourceFs, ct.fileStatus.getPath, targetFs, target, stagingPath,
                ct.matcher, md, codec, successExts, failureExts)
          }
        }
      } catch {
        case e:Throwable =>
          logger.error("{} encountered a generic error", taskName, e:Any)
          Try(Thread.sleep(300000))
      }
      val delay = startTime + interval - System.currentTimeMillis
      if (delay > 0) Try(Thread.sleep(delay))
    }
    "Completed copy"
  }

  def doCopy(conf:Configuration, sourceFs:FileSystem, input: Path, targetFs:FileSystem, target:String,
             stagingPath:Path, matcher:Matcher, md:Option[MessageDigest],codec:Option[CompressionCodec],
             successExts:Array[java.util.Map[String, String] => Boolean],
             failureExts:Array[java.util.Map[String, String] => Boolean],
             meta:AnyRef = null): Unit = try {
    logger.info("{}: To Copy file {}", taskName, input:Any)
    val targetDir = matcher.replaceAll(target)
    val targetDirPath = new Path(targetDir)
    Try(if (!targetFs.exists(targetDirPath)) targetFs.mkdirs(targetDirPath))
    md.foreach(_.reset())
    val targetName = codec match {
      case Some(c) => input.getName + c.getDefaultExtension
      case None => input.getName
    }
    val targetPath = new Path(stagingPath, targetName)
    val destPath = new Path(targetDirPath, targetName)
    if (targetFs.exists(destPath)) {
      logger.info("{}: output {} already exists for {}", taskName, destPath, input)
    } else {
      val outStream = codec match {
        case Some(c) => c.createOutputStream(targetFs.create(targetPath))
        case None => targetFs.create(targetPath)
      }
      IOUtils.copyBytes(sourceFs.open(input), md match {
        case Some(digest) => new DigestOutputStream(outStream, digest)
        case None => outStream
      }, conf)
      val checksum = md.map(_.digest())
      val status = postCopyFunc match {
        case Some(func) => func(targetFs, targetPath, checksum, meta)
        case _ => true
      }
      Try(targetFs.mkdirs(targetDirPath))
      val stats = Map("__source" -> input.toString, "__target" -> destPath.toString, "__fileName" -> input.getName)
      if (status) {
        try {
          targetFs.rename(targetPath, destPath)
          logger.info("{}: Copied from {} to {}", taskName, input, destPath)
          successExts.foreach{ext =>
            try {
              ext(stats)
            } catch {
              case e:Throwable => logger.error("{}: Failed to run success extension", taskName, e:Any)
            }
          }
        } catch {
          case e: Exception =>
            logger.error("{}: Failed to move to {}", taskName, destPath, e)
            failureExts.foreach(ext => Try(ext(stats)))
        }
      } else {
        logger.error("{}: checksum failed for {}", taskName, input:Any)
        failureExts.foreach(ext => Try(ext(stats)))
      }
    }
  } catch {
    case e:Exception =>
      logger.error("{}: Failed to copy", taskName, e:Any)
      val extStats = Map(taskName + ".__source" -> input.toString, taskName +
        ".__target" -> target, taskName + ".__fileName" -> input.getName)
      failureExts.foreach(ext => Try(ext(extStats)))
  }

  override def getProcessorSchema:String = """{"title": "HadoopCopy","type":"object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.HadoopCopy"},
    "source":{"type":"string","description":"Source directory"},
    "target":{"type":"string","description":"Target directory"},
    "pattern":{"type":"string","format":"number","description":"Regular expression of the source files"},
    "recursive":{"type":"boolean","description":"Recursive?"},
    "digest":{"type":"string","description":"Digest method"},
    "beforeCopy":{"type":"string","description":"Function before copy"},
    "afterCopy":{"type":"string","description":"Function after copy"},
    "compressCodec":{"type":"string","description":"Compress codec"},
    "sourceUser":{"type":"string","description":"Alternative user for reading"},
    "sourceKeytab":{"type":"string","description":"Keytab for the source user"},
    "targetUser":{"type":"string","description":"Alternative user for writing"},
    "targetKeytab":{"type":"string","description":"Keytab for the target user"},
    "interval":{"type":"string","format":"number","description":"checking interval in ms"},
    "wait":{"type":"string","format":"number","description":"delay before copying in ms"},
    "staging":{"type":"string","description":"Temporary staging folder"},
    "onSuccess":{"type":"array","format":"tabs","description":"extension after success",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}},
    "onFailure":{"type":"array","format":"tabs","description":"extension after failure",
      "items":{"type":"object","properties":{"__class":{"type":"string"}}}}
    },"required":["__class","source","target"]}"""
}

