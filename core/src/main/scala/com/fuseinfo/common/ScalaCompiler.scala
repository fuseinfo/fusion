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

package com.fuseinfo.common

import java.math.BigInteger
import java.security.MessageDigest

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.internal.util.{AbstractFileClassLoader, BatchSourceFile}
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.{Global, Settings}

class ScalaCompiler {
  private val logger = LoggerFactory.getLogger(classOf[ScalaCompiler])
  private val classMap = mutable.Map.empty[String, Class[_]]
  private val vd =  new VirtualDirectory("_dir", None)
  private val settings = new Settings

  settings.outputDirs.setSingleOutput(vd)
  settings.usejavacp.value = true
  private val global = new Global(settings)
  private lazy val run = new global.Run

  private val classLoader = new AbstractFileClassLoader(vd, this.getClass.getClassLoader)

  /**
    * Compile scala code into a Class. The class name will be automatically generated using MD5
    * @param body Scala source code
    * @tparam T return class
    * @return Java class of the newly compiled code
    */
  def compile[T](body: String): Class[T] = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(body.getBytes)
    val className = "func_" + new BigInteger(1, digest).toString(16)
    getClassByName(className).getOrElse{
      val program = s"class $className $body"
      logger.debug(program)
      val sources = List(new BatchSourceFile("_file", program))
      run.compileSources(sources)
      getClassByName(className).getOrElse{
        logger.error("Failed to compile the code")
        logger.info(program)
        throw new IllegalArgumentException("Unable to compile the code")
      }
    }.asInstanceOf[Class[T]]
  }

  private def getClassByName(className: String): Option[Class[_]] = {
    classMap.get(className).orElse{
      try {
        val clazz = classLoader.loadClass(className)
        classMap(className) = clazz
        Some(clazz)
      } catch {
        case _: ClassNotFoundException => None
      }
    }
  }
}
