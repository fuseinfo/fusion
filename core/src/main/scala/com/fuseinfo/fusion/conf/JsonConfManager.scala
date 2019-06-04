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
package com.fuseinfo.fusion.conf

import scala.collection.JavaConversions._
import java.io.{InputStream, OutputStream, StringWriter}

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

class JsonConfManager(is: InputStream) {
  @transient private val mapper = new ObjectMapper(getJsonFactory)
  @transient private val rootNode = if (is != null) mapper.readTree(is).asInstanceOf[ObjectNode]
                                    else new ObjectNode(new JsonNodeFactory(false))

  def readConf(expanded:Boolean = false): ObjectNode = {
    if (expanded) {
      val expandedRoot = new ObjectNode(new JsonNodeFactory(false))
      loadExtendedConf(rootNode, expandedRoot)
      expandedRoot
    } else {
      rootNode
    }
  }

  private def loadExtendedConf(rootNode: ObjectNode, expandedRoot: ObjectNode):Unit = {
    rootNode.fields().foreach{mapEntry =>
      val fieldName = mapEntry.getKey
      val fieldValue = mapEntry.getValue
      if (fieldName.startsWith("__include_")) {
        val childIs = getClass.getClassLoader.getResourceAsStream(fieldValue.asText())
        val childNode = mapper.readTree(childIs).asInstanceOf[ObjectNode]
        loadExtendedConf(childNode, expandedRoot)
        childIs.close()
      } else {
        expandedRoot.set(fieldName, fieldValue)
      }
    }
  }

  def writeConf(os: OutputStream): Unit = {
    val sw = mapper.writerWithDefaultPrettyPrinter().writeValues(os)
    sw.write(rootNode)
    sw.close()
  }

  def nodeToString(node: ObjectNode): String = {
    val writer = new StringWriter()
    val sw = mapper.writerWithDefaultPrettyPrinter().writeValues(writer)
    sw.write(node)
    writer.flush()
    writer.toString
  }

  def parseToNode(str: String): JsonNode = mapper.readTree(str)

  def getJsonFactory:JsonFactory = new JsonFactory
}

class YamlConfManager(is:InputStream) extends JsonConfManager(is) {
  override def getJsonFactory:JsonFactory = new YAMLFactory
}