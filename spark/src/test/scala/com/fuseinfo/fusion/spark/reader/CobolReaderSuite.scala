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
package com.fuseinfo.fusion.spark.reader

import com.fuseinfo.fusion.spark.SparkBase
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class CobolReaderSuite extends FunSuite with SparkBase  {
  ignore("Read mainframe file"){
    val reader = new CobolReader("MAINFRAME_READER", Map[String, AnyRef]("path" -> "examples/mainframe", "copybook" ->
      """        01  CUSTOMER-RECORD.
           05 CUSTOMER-ID            PIC 9(5) COMP-3.
           05 CUSTOMER-NAME.
              10 CUSTOMER-LAST-NAME  PIC X(15).
              10 CUSTOMER-FIRST-NAME PIC X(10).
           05 ADDRESS.
              10 STREET-LINE         PIC X(20).
              10 CITY                PIC X(20).
              10 STATE               PIC X(02).
              10 OTHER-STATE-NAME    PIC X(20).
              10 COUNTRY             PIC X(3).
              10 ZIP-CODE            PIC X(10).
           05 NOTES                  PIC X(40).""").asJava)
    reader(vars)
    val mainframeDF = spark.table("MAINFRAME_READER")
    assert(mainframeDF.count === 8)
    assert(spark.sql("SELECT SUM(CUSTOMER_ID) FROM MAINFRAME_READER").collect().head.get(0) === 71)
  }
}
