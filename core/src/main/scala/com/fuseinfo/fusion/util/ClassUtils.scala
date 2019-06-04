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
import java.net.URI

import com.google.common.collect.{ImmutableMap, ImmutableSet, ImmutableSortedSet}
import com.google.common.reflect.ClassPath.ResourceInfo
import com.google.common.reflect.ClassPath

import scala.collection.mutable.ArrayBuffer

object ClassUtils {
  def getAllClasses(packageName:String, parent:Class[_] = null): ArrayBuffer[(String, Class[_])] = {
    val f = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    val unsafe = f.get(null).asInstanceOf[sun.misc.Unsafe]
    val getClassPathEntries = classOf[ClassPath]
      .getDeclaredMethod("getClassPathEntries", Array[Class[_]](classOf[ClassLoader]):_*)
    getClassPathEntries.setAccessible(true)
    val empty = unsafe.allocateInstance(classOf[ClassPath])
    val resources = new ImmutableSortedSet.Builder[ResourceInfo](com.google.common.collect.Ordering.usingToString)
    val imap = getClassPathEntries.invoke(empty,
      ClassUtils.getClass.getClassLoader).asInstanceOf[ImmutableMap[URI, ClassLoader]]
    val browse = classOf[ClassPath].getDeclaredMethod("browse",
      Array[Class[_]](classOf[URI], classOf[ClassLoader], classOf[ImmutableSet.Builder[ResourceInfo]]):_*)
    browse.setAccessible(true)
    val it = imap.entrySet.iterator
    while (it.hasNext) {
      val next = it.next
      util.Try(browse.invoke(empty, next.getKey, next.getValue, resources))
    }
    val construct = classOf[ClassPath].getDeclaredConstructor(Array[Class[_]](classOf[ImmutableSet[ResourceInfo]]):_*)
    construct.setAccessible(true)
    val cp = construct.newInstance(resources.build)
    val seq = collection.mutable.ArrayBuffer.empty[(String, Class[_])]
    val is = cp.getTopLevelClasses
    val iterator = is.iterator
    while (iterator.hasNext) {
      val ci = iterator.next
      try {
        val clazz = ci.load
        if ((parent == null || parent.isAssignableFrom(clazz)) &&
          (packageName == null || clazz.getPackage.getName.startsWith(packageName)) &&
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
