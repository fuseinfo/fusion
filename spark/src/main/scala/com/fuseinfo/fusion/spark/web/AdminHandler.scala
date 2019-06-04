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
package com.fuseinfo.fusion.spark.web

import java.util.regex.Pattern

import com.fuseinfo.fusion.spark.FusionHandler
import com.fuseinfo.fusion.Fusion
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.mortbay.jetty.Request

class AdminHandler extends FusionHandler {
  private val actionRegex = Pattern.compile("/+([^/]+)(.*)")

  override def getContext: String = "/admin"

  override def getRoles: Array[String] = Array("admin")

  override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int): Unit= {
    val matcher = actionRegex.matcher(target)
    if (matcher.matches()) {
      matcher.group(1) match {
        case "setStatus" =>
          val remain = matcher.group(2)
          val taskName = remain.substring(remain.indexWhere(c => c != '/'))
          val paramMap = request.getParameterMap
          paramMap.get("status") match {
            case Array(some:String) =>
              val result = setStatus(taskName, some)
              response.setStatus(HttpServletResponse.SC_OK)
              val output = response.getWriter
              output.write(result)
              request match {
                case r: Request => r.setHandled(true)
                case _ =>
              }
            case _ =>
          }
      }
    }
  }

  private def setStatus(taskName:String, status:String): String = {
    try {
      Fusion.setTaskStatus(taskName, status.charAt(0), "Updated by User")
      "Done"
    } catch {
      case e:Exception => e.getMessage
    }
  }
}
