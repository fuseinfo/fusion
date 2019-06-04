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
package com.fuseinfo.fusion.command

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.scalatest.FunSuite

class DateTimeSuite extends FunSuite{
  test("simple") {
    val func = new DateTime
    func("yyyy-MM-dd") == new SimpleDateFormat("yyyy-MM-dd").format(new Date)
  }

  test("yesterday") {
    val func = new DateTime
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    func("yyyy-MM-dd;-1d") == new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  test("month end") {
    val func = new DateTime
    func("yyyy-MM-dd;@1d-1d;2019-03-08") == "2019-02-28"
  }
}
