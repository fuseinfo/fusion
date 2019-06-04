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
package com.fuseinfo.fusion.ext

import com.fuseinfo.fusion.Fusion
import org.slf4j.LoggerFactory

class TaskRunner(params: Map[String, String]) extends (java.util.Map[String, String] => Boolean) {
  private val taskNames = params("taskNames")
  private val background = params.getOrElse("background", "false").toBoolean
  @transient private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(stats: java.util.Map[String, String]): Boolean = {
    try {
      val vars = Fusion.cloneVars()
      params.filter(p => p._1 != "taskNames" && p._1 != "background" && !p._1.startsWith("__"))
        .foreach(p => vars.put(p._1, p._2))
      vars.putAll(stats)
      taskNames.split(";").foreach {taskName =>
        while (Fusion.getTaskStatus(taskName) == 'R') scala.util.Try(Thread.sleep(1000))
        scala.util.Try(Thread.sleep(1000))
        logger.info("Run task {}", taskName)
        Fusion.runTask(taskName, background, vars)
        scala.util.Try(Thread.sleep(1000))
        if (Fusion.getTaskStatus(taskName) == 'F') throw new RuntimeException(s"Failed to run $taskName")
      }
      true
    } catch {
      case e:Throwable => false
    }
  }
}
