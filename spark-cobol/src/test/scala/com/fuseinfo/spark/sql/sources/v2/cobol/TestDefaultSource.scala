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
package com.fuseinfo.spark.sql.sources.v2.cobol

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.FunSuite

class TestDefaultSource extends FunSuite {
  var spark: SparkSession = _
  var sc: SparkContext = _
  implicit var sqlContext: SQLContext = _

    spark = SparkSession.builder
      .master("local")
      .appName("Spark session for testing")
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext

  test("flat layout testing") {
    val df = spark.read
      .format("com.fuseinfo.spark.sql.sources.v2.cobol")
      .option("tree","false")
      .option("copybook",
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
           05 NOTES                  PIC X(40).""")
      .load("examples/mainframe/")
    df.createOrReplaceTempView("CUSTOMERS")
    val output = df.collect.sortBy(_.getInt(0))
    assert(df.count === 8)
    assert(output(0).toString() == "[1,Mouse,Mickey,1 Disney St.,Orlando,FL,,USA,32830,First Customer]")
    assert(output(1).toString() == "[2,Mouse,Minnie,5 Disney St.,Orlando,FL,,USA,32830,The Significant of Mickey]")
    assert(output(2).toString() == "[3,Duck,Donald,7 Disney St.,Orlando,FL,,USA,32830,A funny one]")
    assert(output(3).toString() == "[11,Simpson,Homer,101 Power Pl.,Springfield,MA,,USA,01101,Nuclear Engineer]")
    assert(output(4).toString() == "[12,Simpson,Marge,101 Power Pl.,Springfield,MA,,USA,01101,Housewife]")
    assert(output(5).toString() == "[13,Simpson,Bart,101 Power Pl.,Springfield,MA,,USA,01101,4th grade student]")
    assert(output(6).toString() == "[14,Simpson,Lisa,101 Power Pl.,Springfield,MA,,USA,01101,2nd grade student]")
    assert(output(7).toString() == "[15,Simpson,Maggie,101 Power Pl.,Springfield,MA,,USA,01101,daughter]")
  }

  test("tree layout testing") {
    val df = spark.read
      .format("com.fuseinfo.spark.sql.sources.v2.cobol")
      .option("tree","true")
      .option("copybook",
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
           05 NOTES                  PIC X(40).""")
      .load("examples/mainframe/")
    val output = df.collect.sortBy(_.getStruct(0).getInt(0))
    assert(df.count === 8)
    assert(output(0).toString() == "[[1,[Mouse,Mickey],[1 Disney St.,Orlando,FL,,USA,32830],First Customer]]")
    assert(output(1).toString() == "[[2,[Mouse,Minnie],[5 Disney St.,Orlando,FL,,USA,32830],The Significant of Mickey]]")
    assert(output(2).toString() == "[[3,[Duck,Donald],[7 Disney St.,Orlando,FL,,USA,32830],A funny one]]")
    assert(output(3).toString() == "[[11,[Simpson,Homer],[101 Power Pl.,Springfield,MA,,USA,01101],Nuclear Engineer]]")
    assert(output(4).toString() == "[[12,[Simpson,Marge],[101 Power Pl.,Springfield,MA,,USA,01101],Housewife]]")
    assert(output(5).toString() == "[[13,[Simpson,Bart],[101 Power Pl.,Springfield,MA,,USA,01101],4th grade student]]")
    assert(output(6).toString() == "[[14,[Simpson,Lisa],[101 Power Pl.,Springfield,MA,,USA,01101],2nd grade student]]")
    assert(output(7).toString() == "[[15,[Simpson,Maggie],[101 Power Pl.,Springfield,MA,,USA,01101],daughter]]")
  }
}
