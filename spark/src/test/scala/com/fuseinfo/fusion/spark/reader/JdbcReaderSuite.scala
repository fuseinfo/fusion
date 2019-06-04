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

import java.sql.DriverManager

import com.fuseinfo.fusion.spark.SparkBase
import org.h2.tools.Server
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class JdbcReaderSuite extends FunSuite with SparkBase {
  private val h2 = Server.createTcpServer()
  h2.start()
  private val conn = DriverManager.getConnection ("jdbc:h2:mem:test_mem;DB_CLOSE_DELAY=-1", "sa","")
  private val stmt = conn.createStatement()
  stmt.executeUpdate("CREATE TABLE JDBC_READER AS SELECT * FROM CSVREAD('examples/csv/IRIS.csv')")
  stmt.close()

  test("Read from jdbc"){
    val reader = new JdbcReader("JDBC_READER",
      Map[String, AnyRef]("url" -> "jdbc:h2:mem:test_mem;DB_CLOSE_DELAY=-1", "table" -> "JDBC_READER", "driver" -> "org.h2.Driver",
      "user" -> "sa", "password" -> "").asJava)
    reader(vars)
    val jdbcDF = spark.table("JDBC_READER")
    assert(jdbcDF.count === 150)
    assert(spark.sql("SELECT SUM(CAST(sepal_length AS DECIMAL(8,2))) FROM JDBC_READER").collect()
      .head.getDecimal(0) === new java.math.BigDecimal("876.50"))
  }
}
