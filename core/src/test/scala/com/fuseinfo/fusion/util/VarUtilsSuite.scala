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
package com.fuseinfo.fusion.util

import org.scalatest.FunSuite

class VarUtilsSuite extends FunSuite {
  private val vars = {
    val juVars = new java.util.concurrent.ConcurrentHashMap[String, String]
    juVars.put("e", "p")
    juVars.put("env", "prod")
    juVars
  }

  test("empty string") {
    assert(VarUtils.enrichString("", vars) == "")
  }

  test("no variable") {
    assert(VarUtils.enrichString("a\\$bc", vars) == "a$bc")
  }

  test("single variable") {
    assert(VarUtils.enrichString("/${env}/data/", vars) == "/prod/data/")
  }

  test("multiple variable") {
    assert(VarUtils.enrichString("/${env:-qa}/data/${e}", vars) == "/prod/data/p")
  }

  test("variable with default") {
    assert(VarUtils.enrichString("/${env}/data/${yyyymmdd:-19700101}", vars) == "/prod/data/19700101")
  }

  test("variable with default and escape") {
    assert(VarUtils.enrichString("/${env}/data/${yyyymmdd:-1970\\}01\\}01}", vars) == "/prod/data/1970}01}01")
  }

  test("variable nested") {
    assert(VarUtils.enrichString("/${env}/data/${yyyymmdd:-${abc:-efg}}", vars) == "/prod/data/efg")
  }

  test("single command") {
    assert(VarUtils.enrichString("$(SubStr 4,6,19710203)", vars) == "02")
  }

  test("command with variable") {
    val tVars = new java.util.concurrent.ConcurrentHashMap[String, String](vars)
    tVars.put("yyyymmdd", "19700203")
    assert(VarUtils.enrichString("$(SubStr 4,6,${yyyymmdd})", tVars) == "02")
  }

  test("nested with variable") {
    val tVars = new java.util.concurrent.ConcurrentHashMap[String, String](vars)
    tVars.put("yyyymmdd", "19700203")
    assert(VarUtils.enrichString("begin ${MyVar:-$(SubStr 4,6,${yyyymmdd})} end", tVars) == "begin 02 end")
  }

  test("nested with variable with default") {
    val tVars = new java.util.concurrent.ConcurrentHashMap[String, String](vars)
    tVars.put("yyyymmdd", "19700203")
    tVars.put("MyVar", "03")
    assert(VarUtils.enrichString("begin ${MyVar:-$(SubStr 4,6,${yyyymmdd})} end", tVars) == "begin 03 end")
  }
}


