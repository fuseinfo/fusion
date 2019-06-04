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
package com.fuseinfo.fusion.spark.writer

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class CsvWriterSuite extends FunSuite with DataWriterBase {
  test("Write to CSV file") {
    val outputDir = "./tmp/CsvWriter/output"
    val stagingDir = "./tmp/CsvWriter/staging"
    val outputPath = new Path(outputDir)
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    scala.util.Try(fs.delete(outputPath, true))
    scala.util.Try(fs.delete(new Path(stagingDir), true))

    val writer = new CsvWriter("CSV_WRITER", Map[String, AnyRef]("path" -> outputDir, "table" -> "DATA_WRITER",
      "staging" -> stagingDir, "coalesce" -> "1", "header" -> "true").asJava)
    writer(vars)
    val csvFiles = fs.listStatus(outputPath)
    assert(csvFiles.size === 1)
    val is = fs.open(csvFiles.head.getPath)
    val br = new BufferedReader(new InputStreamReader(is))
    val out = Stream.continually(br.readLine).takeWhile(_ != null).sorted
    assert(out(0).startsWith("1,foo,1970-01-01,12.34,2018-01-0"))
    assert(out(1).startsWith("2,bar,1980-02-02,56.78,2018-02-0"))
    assert(out(2) == "id,name,dob,amt,lud")
  }
}
