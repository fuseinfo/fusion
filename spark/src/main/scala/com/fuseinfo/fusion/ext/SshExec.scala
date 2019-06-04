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
package com.fuseinfo.fusion.ext

import java.io.{File, FileInputStream}
import java.util.Properties

import com.fuseinfo.fusion.Fusion
import com.fuseinfo.fusion.util.VarUtils
import com.jcraft.jsch.{ChannelExec, JSch}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class SshExec(params: Map[String, String]) extends (java.util.Map[String, String] => Boolean) {
  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(stats: java.util.Map[String, String]): Boolean = {
    val vars = Fusion.cloneVars()
    vars.putAll(stats)
    val enrichedParams = params.mapValues(VarUtils.enrichString(_, vars))
    val jsch = new JSch
    val user = enrichedParams("user")
    val session = jsch.getSession(user, enrichedParams.getOrElse("host", "localhost"),
      enrichedParams.getOrElse("port","22").toInt)
    val pass = enrichedParams.get("password")
    enrichedParams.get("identity") match {
      case Some(identityPath) =>
        val is = this.getClass.getClassLoader.getResourceAsStream(identityPath) match {
          case null =>
            val identityFile = new File(identityPath)
            if (identityFile.isFile) new FileInputStream(identityFile) else {
              val prvPath = new Path(identityPath)
              val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.get)
              val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
              fs.open(prvPath)
            }
          case embIS => embIS
        }
        val byteArray = new Array[Byte](10240)
        var off = 0
        do {
          val size = is.read(byteArray, off, 10240 - off)
          if (size > 0) off += size else {
            val prvKey = new Array[Byte](off)
            System.arraycopy(byteArray, 0, prvKey, 0, off)
            val passphrase = pass match {
              case Some(passStr) => passStr.getBytes
              case None => Array.emptyByteArray
            }
            jsch.addIdentity(user, prvKey, null, passphrase)
            off = -1
          }
        } while (off >= 0)
      case None => pass.foreach(session.setPassword)
    }
    val props = new Properties()
    props.put("StrictHostKeyChecking", "no")
    session.setConfig(props)
    session.connect(10000)
    val channel = session.openChannel("exec").asInstanceOf[ChannelExec]
    val command = enrichedParams("command")
    channel.setCommand(command)
    channel.setInputStream(null)
    val in = channel.getInputStream
    channel.connect()
    val buffer = new Array[Byte](1024)
    do {
      scala.util.Try(Thread.sleep(1000))
      while (in.available > 0) {
        val size = in.read(buffer, 0, 1024)
        if (size > 0 && logger.isDebugEnabled) logger.debug(new String(buffer, 0, size, "Cp1252"))
      }
    } while (!channel.isClosed)
    channel.disconnect()
    session.disconnect()
    true
  }
}
