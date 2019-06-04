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

class SubStr extends (String => String){
  override def apply(param: String): String = {
    val first = param.indexOf(',')
    val second = param.indexOf(',', first + 1)
    val rest = param.substring(second + 1).trim
    val len = rest.length
    val begin = param.substring(0, first).trim.toInt match {
      case i if i < 0 => len + i
      case i => i
    }
    val end = param.substring(first + 1, second).trim match {
      case "" => len
      case s => s.toInt match {
        case i if i < 0 => len + i
        case i => i
      }
    }
    rest.substring(if (begin < 0) 0 else begin, if(end > len) len else  end)
  }
}
