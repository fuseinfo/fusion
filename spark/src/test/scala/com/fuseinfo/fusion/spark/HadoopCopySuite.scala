/*
 * Copyright 2018 Fuseinfo Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fuseinfo.fusion.spark

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class HadoopCopySuite extends FunSuite with SparkBase with SftpBase {
  test("test from sftp to local") {
    val conf = spark.sparkContext.hadoopConfiguration
    conf.set("fs.sftp.impl", "org.apache.hadoop.fs.sftp.SftpFileSystem")
    conf.set("fs.sftp.pass.localhost", "unittest")

    val userDir = System.getProperty("user.dir")
    val baseDir = if (userDir.length > 1 && userDir.charAt(1) == ':') userDir.substring(2) else userDir
    val stdDir = baseDir.replace("\\", "/")
    val outDir = "file://" + stdDir + "/tmp/HadoopCopy/out"
    val outDirPath = new Path(outDir)
    val fs = FileSystem.get(new URI(outDir), conf)
    scala.util.Try(fs.delete(outDirPath, true))
    scala.util.Try(fs.mkdirs(outDirPath))
    val thread = new Thread(new Runnable(){
      override def run(): Unit = {
        val hc = new HadoopCopy("Copy", Map[String, AnyRef]("source" -> ("sftp://unittest@localhost:14022" + stdDir + "/examples/csv"),
          "pattern" -> ".+\\.csv", "target" -> outDir, "interval" -> "1000", "wait" -> "1000").asJava)
        hc(vars)
      }
    })
    thread.start()
    scala.util.Try(Thread.sleep(20000))
    assert(fs.getFileStatus(new Path(outDirPath, "IRIS.csv")).isFile)
  }
}
