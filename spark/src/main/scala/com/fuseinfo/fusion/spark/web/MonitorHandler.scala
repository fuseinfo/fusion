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
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.server.Request

class MonitorHandler extends FusionHandler {

  def loadStatus: String = {
    val statusSeq = Fusion.getAllTaskStatus.filter(_._1.trim != "")
    val size = statusSeq.size
    "{\"draw\": 1,\"recordsTotal\":" + size + ",\"recordsFiltered\":" + size + ",\"data\":[" +
    statusSeq.map{case (t, s) => "[\"" + StringEscapeUtils.escapeHtml(t) + "\",\"" + s + "\"]"}.mkString(",") + "]}"
  }

  override def getContext: String = "/status"

  override def getRoles: Array[String] = Array("user")

  private val actionRegex = Pattern.compile("/+([^/]+)(.*)")

  override def handle(target: String, r:Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {
    val matcher = actionRegex.matcher(target)
    if (matcher.matches()) {
      response.setStatus(HttpServletResponse.SC_OK)
      val output = response.getWriter
      val result = matcher.group(1) match {
        case "loadStatus" => loadStatus
        case "info" => getInfo
        case _ => ""
      }
      output.write(result)
      r.setHandled(true)
    }
  }

  private def getInfo: String = {
    val spark = SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.orNull)
    if (spark != null) {
      val sc = spark.sparkContext
      val memoryStatus = sc.getExecutorMemoryStatus.toSeq.sortBy(_._1)
      "<table class='table table-striped'><thead><tr><th>Node</th><th>Max</th><th>Free</th></tr></thead>" +
      memoryStatus.map{case (node, (max, free)) =>
        s"<tr><td>$node</td><td>$max</td><td>$free</td></tr>"
      }.mkString("") + "</tbody></table>"
    } else ""
  }
}
