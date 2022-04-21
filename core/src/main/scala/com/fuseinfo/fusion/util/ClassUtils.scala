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
package com.fuseinfo.fusion.util

import java.lang.reflect.Modifier
import com.google.common.reflect.ClassPath

import scala.collection.mutable.ArrayBuffer

object ClassUtils {
  def getAllClasses(packageName:String, parent:Class[_] = null): ArrayBuffer[(String, Class[_])] = {
    val cp = ClassPath.from(getClass.getClassLoader)
    val is = if (packageName == null) cp.getTopLevelClasses else cp.getTopLevelClassesRecursive(packageName)
    val seq = collection.mutable.ArrayBuffer.empty[(String, Class[_])]
    val iterator = is.iterator
    while (iterator.hasNext) {
      val ci = iterator.next
      try {
        val clazz = ci.load
        if ((parent == null || parent.isAssignableFrom(clazz)) &&
          !Modifier.isAbstract(clazz.getModifiers)) seq.append((clazz.getSimpleName, clazz))
      } catch {
        case _:Throwable =>
      }
    }
    seq
  }

  def newExtension(className: String, props:Map[String, String]): java.util.Map[String, String] => Boolean = {
    val clazz = try {
      Class.forName(className)
    } catch {
      case _:ClassNotFoundException => Class.forName("com.fuseinfo.fusion." + className)
    }
    try {
      clazz.getConstructor(classOf[Map[String, String]])
        .newInstance(props).asInstanceOf[java.util.Map[String, String] => Boolean]
    } catch{
      case _:Exception => clazz.newInstance.asInstanceOf[java.util.Map[String, String] => Boolean]
    }
  }
}
