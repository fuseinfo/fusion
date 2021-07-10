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

import java.nio.file.{FileSystems, Files, Paths}
import java.util
import java.util.Collections
import java.util.regex.Pattern
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}
import com.fuseinfo.common.conf.ConfUtils
import com.fuseinfo.fusion.Fusion
import com.fuseinfo.fusion.spark.FusionHandler
import com.fuseinfo.fusion.util.ClassUtils
import org.eclipse.jetty.server.Request

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConversions._

class ConfigHandler extends FusionHandler {
  private val mapperYaml = new ObjectMapper({
    val factory = new YAMLFactory()
    factory.enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
    factory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
    factory.disable(YAMLGenerator.Feature.SPLIT_LINES)
  })

  private val mapperJson = new ObjectMapper

  override def getContext: String = "/config"

  override def getRoles: Array[String] = Array("admin")

  private val funcGroupList = ClassUtils.getAllClasses(null, classOf[util.Map[String, String] => String])
    .filter{case (_, clazz) => scala.util.Try(clazz.getMethod("getProcessorSchema")).isSuccess}
    .map { case (className, clazz) =>
      val packName = clazz.getPackage.getName
      (if (packName == "com.fuseinfo.fusion") "fusion"
      else if (packName.startsWith("com.fuseinfo.fusion.")) packName.substring(20)
      else packName, className, clazz)
    }.groupBy(_._1).map(p => p._1 -> p._2.map(v => (v._2, v._3)).sortBy(_._1)).toSeq.sortBy(_._1)

  private val actionRegex = Pattern.compile("/+([^/]+)(.*)")

  override def handle(target: String, r:Request, request: HttpServletRequest, response: HttpServletResponse): Unit = {

    val matcher = actionRegex.matcher(target)
    if (matcher.matches()) {
      val result = matcher.group(1) match {
        case "load" => load(request, response)
        case "list" => list(request, response)
        case "download" => download(request, response)
        case "save" => save(request, response)
        case "schema" => getSchema(matcher.group(2), request, response)
        case "play" => play(request, response)
        case "upload" => upload(request, response)
        case _ => false
      }
      r.setHandled(result)
    }
  }

  private def list(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setContentType("application/html")
    val writer = response.getWriter
    writer.write(
      funcGroupList.map { case (packName, list) =>
        val id = packName.replace('.', '_')
        s"<button class='btn btn-primary btn-block' data-toggle='collapse' data-target='#$id'>$packName</button><div id='$id' class='collapse'>" +
          list.map { case (className, clazz) => s"<span class='btn btn-info task-processor col-lg-12' id='${clazz.getCanonicalName}' data-toggle='modal' data-target='#task_Modal'>$className</span>" }.mkString("") +
          "</div>"
      }.mkString(""))

    if (userProcessors.nonEmpty) {
      writer.write("<button class='btn btn-primary btn-block' data-toggle='collapse' data-target='#userProcessor'>User Processors</button><div id='userProcessor' class='collapse'>")
      writer.write(userProcessors)
      writer.write("</div>")
    }
    response.setStatus(HttpServletResponse.SC_OK)
    true
  }

  private def getSchema(suffix: String, request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val begin = suffix.indexWhere(_ != '/')
    val idx = suffix.indexOf('/', begin)
    val className = suffix.substring(begin, idx)
    val taskName = suffix.substring(idx + 1)
    response.setContentType("application/text")
    val schema = if (className.charAt(0) == '_')
      scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("processors/" + className.substring(1) + ".json")).mkString
    else {
      val task = Class.forName(className).getDeclaredConstructor(classOf[String]).newInstance(taskName)
      scala.util.Try(task.getClass.getMethod("getProcessorSchema").invoke(task).toString).getOrElse(getProcessorSchema)
    }
    response.getWriter.write(schema)
    true
  }

  private def download(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setContentType("application/octet-stream")
    response.setHeader("Content-Disposition", "attachment; filename=Fusion_" + System.currentTimeMillis + ".yaml")
    val paramMap = request.getParameterMap
    val rootNode = mapperYaml.createObjectNode()
    paramMap.get("__list")(0).split("\\|").map(_.trim).foreach { taskName =>
      val jsonNode = mapperJson.readTree(paramMap.get(taskName)(0))
      Fusion.addTask(taskName, jsonNode)
      rootNode.set(taskName, jsonNode)
    }
    val writer = mapperYaml.writerWithDefaultPrettyPrinter.writeValues(response.getWriter)
    writer.write(rootNode)
    response.setStatus(HttpServletResponse.SC_OK)
    writer.flush()
    true
  }

  private def save(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    val paramMap = request.getParameterMap
    paramMap.get("__list")(0).split("\\|").map(_.trim).foreach { taskName =>
      val jsonNode = mapperJson.readTree(paramMap.get(taskName)(0))
      Fusion.addTask(taskName, jsonNode)
    }
    response.setStatus(HttpServletResponse.SC_OK)
    true
  }

  private def play(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setStatus(HttpServletResponse.SC_OK)
    val writer = response.getWriter
    val objNode = mapperJson.readTree(request.getParameterValues("json")(0)).asInstanceOf[ObjectNode]
    val taskName = request.getParameterValues("taskName")(0)
    val action = objNode.get("__class").asText()
    try {
      val params = ConfUtils.jsonToMap(objNode)
      Fusion.removeTask(taskName)
      Fusion.addTask(taskName, objNode)
      Fusion.runWithDependency(taskName, action, params)
      assert(Fusion.getTaskStatus(taskName) == 'P')
      response.setStatus(HttpServletResponse.SC_OK)
      writer.write(s"Executed $taskName as $action")
    } catch {
      case e: Throwable =>
        writer.write(s"Failed to run $taskName as $action due to ${e.getMessage}")
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    true
  }

  private def load(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setContentType("text/javascript")
    val writer = response.getWriter
    writer.write(
      "function addItems() {\n" +
        Fusion.iterateTask.map { case (taskName, jsonNode) =>
          val json = jsonNode.toString.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "").replace("\r", "")
          val func = Fusion.getFunction(taskName)
          val task = if (func != null) func
          else {
            val className = jsonNode.get("__class").asText
            val clazz = try {
              Class.forName(className)
            } catch {
              case _:ClassNotFoundException => Class.forName("com.fuseinfo.fusion." + className)
            }
            clazz.getDeclaredConstructor(classOf[String]).newInstance(taskName)
          }
          val schema = scala.util.Try(task.getClass.getMethod("getProcessorSchema").invoke(task).toString)
            .getOrElse(getProcessorSchema).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "").replace("\r", "")
          "createTask(\"" + taskName + "\", \"" + schema + "\", \"" + json + "\");"
        }.mkString("\n") +
        "}\n")
    response.setStatus(HttpServletResponse.SC_OK)
    true
  }

  private def upload(request: HttpServletRequest, response: HttpServletResponse): Boolean = {
    response.setContentType("text/javascript")
    val writer = response.getWriter

    val str = try {
      val br = new java.io.BufferedReader(new java.io.InputStreamReader(request.getInputStream))
      val boundary = br.readLine
      val header = br.readLine
      while (br.readLine.nonEmpty){}
      val sb = new StringBuilder
      var line = ""
      while (line != null && !line.startsWith(boundary)) {
        sb.append(line).append("\n")
        line = br.readLine()
      }
      br.close()
      sb.toString
    } catch {
      case e:Exception => ""
    }

    val rootNode = mapperYaml.readTree(str)
    writer.write(rootNode.fields().map { pair =>
      val taskName = pair.getKey
      val jsonNode = pair.getValue
      Fusion.addTask(taskName, jsonNode)
      val func = Fusion.loadTask(taskName)
      val json = jsonNode.toString.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "").replace("\r", "")
      val schema = scala.util.Try(func.getClass.getMethod("getProcessorSchema").invoke(func).toString)
        .getOrElse(getProcessorSchema).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "").replace("\r", "")
      "createTask(\"" + taskName + "\", \"" + schema + "\", \"" + json + "\");"
    }.mkString("\n"))
    true
  }

  private val userProcessors = {
    val sb = new StringBuilder
    val procUrl = getClass.getResource("/processors")
    if (procUrl != null) {
      val procUri = procUrl.toURI
      val path = if (procUri.getScheme != "jar") Paths.get(procUri)
      else FileSystems.newFileSystem(procUri, Collections.emptyMap[String, AnyRef]).getPath("/processors")
      val iterator = Files.walk(path, 1).iterator
      while (iterator.hasNext) {
        val pt = iterator.next
        val fileName = pt.getName(pt.getNameCount - 1).toString
        if (fileName.endsWith(".json")) {
          val name = fileName.substring(0, fileName.length - 5)
          sb.append(s"<span class='btn btn-info task-processor col-lg-12' id='_$name' data-toggle='modal' data-target='#task_Modal'>$name</span>")
        }
      }
    }
    sb.toString
  }

  private def getProcessorSchema: String =
    """{"title": "Function","type": "object","properties": {"__class":{"type":"string","options":{"hidden":true}}},"required":["__class"]}"""
}