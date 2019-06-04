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

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.fuseinfo.fusion.Fusion
import com.fuseinfo.fusion.spark.FusionHandler
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.commons.lang.StringEscapeUtils
import org.mortbay.jetty.Request

class LogHandler extends FusionHandler {
  override def getContext: String = "/log"

  override def getRoles: Array[String] = Array("log")

  override def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int): Unit = {
    response.setStatus(HttpServletResponse.SC_OK)
    val writer = response.getWriter

    val size = Fusion.getLogSize
    val zoneId = ZoneId.systemDefault
    writer.write("{\"draw\": 1,\"recordsTotal\":" + size + ",\"recordsFiltered\":" + size + ",\"data\":[" +
    Fusion.getLogs.map{case (ts, taskName, status, message) =>
      "[\"" + ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), zoneId).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) +
      "\",\"" + StringEscapeUtils.escapeHtml(taskName) + "\",\"" + (status match {
        case 'P' => "Passed"
        case 'F' => "Failed"
        case 'C' => "Cancelled"
        case __ => status
      }) + "\",\"" + StringEscapeUtils.escapeHtml(message) + "\"]"}.mkString(",") + "]}")
    request match {
      case r: Request => r.setHandled(true)
      case _ =>
    }
  }
}
