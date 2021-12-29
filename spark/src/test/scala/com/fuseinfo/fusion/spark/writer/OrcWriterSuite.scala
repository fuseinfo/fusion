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

import java.time.LocalDate

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.OrcFile
import org.apache.hadoop.hive.ql.exec.vector._
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class OrcWriterSuite extends FunSuite with DataWriterBase {
  test("Write to Orc file"){
    val outputDir = "./tmp/OrctWriter/output"
    val stagingDir = "./tmp/OrcWriter/staging"
    val outputPath = new Path(outputDir)
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    scala.util.Try(fs.delete(new Path(outputDir), true))
    scala.util.Try(fs.delete(new Path(stagingDir), true))
    spark.sql("SET spark.sql.orc.impl=native")
    val writer = new OrcWriter("ORC_WRITER",
      Map[String, AnyRef]("path" -> outputDir, "table"->"DATA_WRITER", "staging" -> stagingDir, "coalesce"->"1").asJava)
    writer(vars)
    val orcFiles = fs.listStatus(outputPath)
    assert(orcFiles.size === 1)

    val reader = OrcFile.createReader(orcFiles.head.getPath, OrcFile.readerOptions(conf))
    val rows = reader.rows
    val batch = reader.getSchema.createRowBatch

    val out = new Iterator[VectorizedRowBatch](){
      override def hasNext: Boolean = rows.nextBatch(batch)
      override def next(): VectorizedRowBatch = batch
    }.flatMap{rowBatch =>
      for (i <- 0 until rowBatch.size)
        yield {
          val tsv = rowBatch.cols(4).asInstanceOf[TimestampColumnVector]
          val ts = new java.sql.Timestamp(tsv.time(i))
          ts.setNanos(tsv.nanos(i))
          ( rowBatch.cols(0).asInstanceOf[LongColumnVector].vector(i),
            rowBatch.cols(1).asInstanceOf[BytesColumnVector].toString(i),
            LocalDate.ofEpochDay(rowBatch.cols(2).asInstanceOf[LongColumnVector].vector(i)) ,
            rowBatch.cols(3).asInstanceOf[DecimalColumnVector].vector(i).getHiveDecimal.bigDecimalValue(),
            ts)
        }
    }.toList.sortBy(_._1)
    assert(out.head._1 === 1)
    assert(out.head._2 === "foo")
    assert(out.head._3.toString == "1970-01-01")
    assert(out.head._4 === new java.math.BigDecimal("12.34"))
    assert(out.head._5.toString.startsWith("2018-01-0"))
    assert(out(1)._1 === 2)
    assert(out(1)._2 === "bar")
    assert(out(1)._3.toString == "1980-02-02")
    assert(out(1)._4 === new java.math.BigDecimal("56.78"))
    assert(out(1)._5.toString.startsWith("2018-02-0"))
  }
}
