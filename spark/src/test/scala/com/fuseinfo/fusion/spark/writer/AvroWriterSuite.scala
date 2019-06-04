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

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class AvroWriterSuite extends FunSuite with DataWriterBase{

  test("Write to Avro file"){
    val outputDir = "./tmp/AvroWriter/output"
    val stagingDir = "./tmp/AvroWriter/staging"
    val outputPath = new Path(outputDir)
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    scala.util.Try(fs.delete(outputPath, true))
    scala.util.Try(fs.delete(new Path(stagingDir), true))

    val writer = new AvroWriter("AVRO_WRITER",
      Map[String, AnyRef]("path" -> outputDir, "table"->"DATA_WRITER", "staging" -> stagingDir, "coalesce"->"1").asJava)
    writer(vars)
    val avroFiles = fs.listStatus(outputPath)
    assert(avroFiles.size === 1)
    val out = getAvroRecords(avroFiles.head.getPath, conf)(record =>
      (record.get("id").asInstanceOf[java.lang.Long],
        record.get("name").asInstanceOf[Utf8].toString,
      record.get("dob").asInstanceOf[java.lang.Long],
      record.get("amt").asInstanceOf[Utf8].toString,
      record.get("lud").asInstanceOf[java.lang.Long])
    ).toList.sortBy(_._1)
    assert(out(0)._1 === 1)
    assert(out(0)._2.toString === "foo")
    assert(new java.sql.Date(out(0)._3).toString == "1970-01-01")
    assert(out(0)._4 === "12.34")
    assert(out(0)._5 === 1514851200000L)
    assert(out(1)._1 === 2)
    assert(out(1)._2.toString === "bar")
    assert(new java.sql.Date(out(1)._3).toString == "1980-02-02")
    assert(out(1)._4 === "56.78")
    assert(out(1)._5 === 1517565600000L)
  }

  private def getAvroRecords[T](path:Path, conf:Configuration)(rt:GenericRecord => T) = {
    val datum = new GenericDatumReader[GenericRecord]
    val reader = new DataFileReader[GenericRecord](new FsInput(path, conf), datum)
    val record = new GenericData.Record(reader.getSchema)
    new Iterator[T] {
      def hasNext: Boolean = reader.hasNext
      def next: T = {
        reader.next(record)
        rt(record)
      }
    }
  }
}
