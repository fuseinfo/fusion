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
package com.fuseinfo.fusion.command

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.regex.Pattern

class DateTime extends (String => String){
  private val pattern = Pattern.compile("[+-@]\\d+[yMdHmsS]")

  override def apply(param: String): String = {
    val first = param.indexOf(';')
    if (first < 0) {
      new SimpleDateFormat(param).format(new Date)
    } else {
      val sdf = new SimpleDateFormat(param.substring(0, first))
      val second = param.indexOf(';', first + 1)
      val (date, adj) = if (second > first)
        (sdf.parse(param.substring(second + 1)), param.substring(first + 1, second))
      else (new Date, param.substring(first + 1))

      val matcher = pattern.matcher(adj)
      val cal = Calendar.getInstance()
      cal.setTime(date)
      while (matcher.find()) {
        val region = matcher.group(0)
        val last = region.length - 1
        val amount = region.substring(1, last).toInt
        val field = region.charAt(last) match {
          case 'y' => Calendar.YEAR
          case 'M' => Calendar.MONTH
          case 'd' => Calendar.DAY_OF_MONTH
          case 'H' => Calendar.HOUR_OF_DAY
          case 'm' => Calendar.MINUTE
          case 's' => Calendar.SECOND
          case 'S' => Calendar.MILLISECOND
        }
        region.charAt(0) match {
          case '@' => cal.set(field, amount)
          case '+' => cal.add(field, amount)
          case '-' => cal.add(field, -amount)
        }
      }
      sdf.format(cal.getTime)
    }
  }
}
