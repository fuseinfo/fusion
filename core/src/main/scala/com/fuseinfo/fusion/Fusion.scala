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
package com.fuseinfo.fusion

import java.io.{File, FileInputStream}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, Executors, Future}
import java.util.function.{Consumer, Supplier}

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode, ValueNode}
import com.fuseinfo.common.conf.ConfUtils
import com.fuseinfo.fusion.conf.YamlConfManager
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

object Fusion extends App {
  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private var ConfigName = "Fusion.yaml"
  protected val taskStatusMap = collection.concurrent.TrieMap.empty[String, Char]
  protected val vars = new ConcurrentHashMap[String, String]()
  private val execCache = new ConcurrentHashMap[String, CompletableFuture[_]]()
  private val futureCache = new ConcurrentHashMap[String, Future[_]]()
  protected val executorService = Executors.newFixedThreadPool(1000)
  private val packageName = getClass.getPackage.getName
  val funcCache = new ConcurrentHashMap[String, FusionFunction]()
  private val taskLog = new Array[(Long, String, Char, String)](1000)
  private var taskLogIndex = 0

  args.foreach{arg =>
    val pos = arg.indexOf('=')
    if (pos > 0 && arg.charAt(0) != '-') {
      vars.put(arg.substring(0, pos), arg.substring(pos + 1))
    } else {
      if (arg.startsWith("--config=")) {
        ConfigName = arg.substring(9).trim
      }
    }
  }
  logger.info("Starting Fusion using {}", ConfigName)

  val rootIs = getClass.getClassLoader.getResourceAsStream(ConfigName) match {
    case null => Try(new FileInputStream(new File(ConfigName))).getOrElse(null)
    case is => is
  }
  val confManager = new YamlConfManager(rootIs)
  val rootNode =  confManager.readConf(true)
  Try(rootIs.close())

  private val taskMap = mutable.LinkedHashMap.empty[String, JsonNode]
  val itTasks = rootNode.fields()
  var lastTask:String = _
  val backgroundSet = mutable.Set.empty[String]

  while (itTasks.hasNext) {
    val taskEntry = itTasks.next()
    val entryName = taskEntry.getKey.trim
    val background = entryName.endsWith("&")
    val taskName = if (background) entryName.substring(0, entryName.length - 1).trim else entryName
    if (background) backgroundSet.add(taskName)
    if (!taskName.startsWith("__")) {
      taskMap.put(taskName, taskEntry.getValue)
      val baseMap = new java.util.HashMap[String, AnyRef]
      if (lastTask != null) baseMap.put("__previous", lastTask)
      loadTask(taskName, taskEntry.getValue, baseMap)
      lastTask = taskName
    }
  }

  taskMap.foreach{taskEntry => processTask(taskEntry._1)}

  args.foreach{arg =>
    val isBackground = arg.startsWith("-B")
    val isForeground = arg.startsWith("-F")
    if (isBackground || isForeground) {
      val pos = arg.indexOf('=')
      if (pos >= 2) {
        val funcName = arg.substring(pos + 1)
        processTask(arg.substring(2, pos), if (isBackground) funcName + "&" else funcName,
          new java.util.HashMap[String, AnyRef])
      }
    }
  }

  while (taskStatusMap.exists{case (_, v) => v != 'P' && v != 'F'}) {
    Try(Thread.sleep(500))
  }
  funcCache.foreach(func => Try(func._2.close()))
  executorService.shutdown()

  private[fusion] def addTask(taskName:String, jsonNode:JsonNode) = {
    val isNew = taskMap.get(taskName) match {
      case Some(oldValue) => !oldValue.equals(jsonNode)
      case None => true
    }
    if (isNew) {
      taskMap.put(taskName, jsonNode)
      funcCache.remove(taskName)
      loadTask(taskName)
    }
  }

  private[fusion] def iterateTask = taskMap.toIterator

  private[fusion] def getFunction(taskName:String): FusionFunction = funcCache.get(taskName)

  private def loadTask(taskName:String, jsonNode:JsonNode, baseMap:java.util.Map[String, AnyRef]) = {
    jsonNode match {
      case objNode: ObjectNode =>
        val className = objNode.get("__class") match {
          case null => throw new IllegalArgumentException(s"__class is not set for $taskName")
          case valueNode: ValueNode => valueNode.asText.trim
          case _ => throw new IllegalArgumentException(s"__class can not be an object for $taskName")
        }
        val params = ConfUtils.jsonToMap(objNode)
        params.putAll(baseMap)
        initTask(taskName, className, params)
      case textNode: TextNode =>
        val className = textNode.asText.trim
        initTask(taskName, className, baseMap)
      case _ => throw new IllegalArgumentException(s"invalid configuration for $taskName")
    }
  }

  private[fusion] def loadTask(taskName:String): FusionFunction =
    loadTask(taskName, taskMap(taskName), new java.util.HashMap[String, AnyRef])

  private def initTask(taskName:String, className:String, params:java.util.Map[String, AnyRef]): FusionFunction = {
    funcCache.get(taskName) match {
      case null =>
        val f = (try {
          val clazz = Try(Class.forName(className)).getOrElse(Class.forName(packageName + "." + className))
          clazz.getConstructor(classOf[String], classOf[java.util.Map[String, AnyRef]]).newInstance(taskName, params)
        } catch {
          case e:Exception =>
            setTaskStatus(taskName, 'F', s"Unable to create an instance of class $className")
            logger.error("Unable to create an instance of class {}", className, e:Any)
            null
        }).asInstanceOf[FusionFunction]
        funcCache.put(taskName, f)
        f
      case f => f
    }
  }

  private def processTask(taskName:String, funcStr:String, params:java.util.Map[String, AnyRef]): Unit = {
    taskStatusMap.put(taskName, 'W')
    if (funcStr.endsWith("&")) {
      logger.info("Queuing {} in background", taskName)
      val future = executorService.submit(new Runnable() {
        override def run(): Unit =
          runWithDependency(taskName, funcStr.substring(0, funcStr.length - 1).trim, params)
      })
      futureCache.put(taskName, future)

    } else {
      logger.info("Processing {}, checking dependencies", taskName)
      runWithDependency(taskName, funcStr, params)
    }
  }

  private def processTask(taskName:String): Unit = {
    taskStatusMap.put(taskName, 'W')
    if (backgroundSet.contains(taskName)) {
      logger.info("Queuing {} in background", taskName)
      val future = executorService.submit(new Runnable() {
        override def run(): Unit =
          runWithDependency(taskName)
      })
      futureCache.put(taskName, future)
    } else {
      logger.info("Processing {}, checking dependencies", taskName)
      runWithDependency(taskName)
    }
  }

  private[fusion] def runWithDependency(taskName:String): Unit = {
    val depsNode = taskMap.get(taskName).map(_.get("__deps")).orNull

    val background = backgroundSet.contains(taskName)
    val func = funcCache.get(taskName) match {
      case null => loadTask(taskName)
      case f => f
    }
    if (depsNode != null) {
      val deps = depsNode match {
        case arrayNode:ArrayNode => arrayNode.elements().map(_.asText).toArray
        case valueNode:ValueNode => valueNode.asText.split(",").map(_.trim)
      }
      while(taskStatusMap.getOrElse(taskName, 'W') == 'W' &&
        deps.exists(parent => taskStatusMap.getOrElse(parent, 'W') != 'P' )) Try(Thread.sleep(50))
    }

    if (func != null && taskStatusMap.getOrElse(taskName, 'W') == 'W') {
      runTask(taskName, background, vars)
    }
  }

  private[fusion] def runWithDependency(taskName:String, functionStr:String, params:java.util.Map[String, AnyRef]): Unit = {
    val pipePos = functionStr.indexOf('|')
    val (classStr, deps) = if (pipePos > 0) {
      (functionStr.substring(0, pipePos).trim, functionStr.substring(pipePos + 1).split(",").map(_.trim))
    } else (functionStr, Array.empty[String])
    val background = classStr.endsWith("&")
    val className = if (background) classStr.substring(0, classStr.length - 1).trim else classStr
    val clazz = Try(Class.forName(className)).getOrElse(Class.forName(packageName + "." + className))
    val func = funcCache.get(taskName) match {
      case null =>
        val f = (try {
          clazz.getConstructor(classOf[String], classOf[java.util.Map[String, AnyRef]]).newInstance(taskName, params)
        } catch {
          case e:Exception =>
            setTaskStatus(taskName, 'F', s"Unable to create an instance of class $className")
            logger.error("Unable to create an instance of class {}", className, e:Any)
            null
        }).asInstanceOf[FusionFunction]
        funcCache.put(taskName, f)
        f
      case f => f
    }
    while(taskStatusMap.getOrElse(taskName, 'W') == 'W' &&
      deps.exists(parent => taskStatusMap.getOrElse(parent, 'W') != 'P' )) Try(Thread.sleep(50))

    if (func != null && taskStatusMap.getOrElse(taskName, 'W') == 'W') {
      runTask(taskName, background, vars)
    }
  }

  private[fusion] def runTask(taskName: String, background:Boolean, runVars:java.util.Map[String, String]): Unit = {
    taskStatusMap.put(taskName, 'R')
    val func = funcCache(taskName)
    logger.info("{} Running {}{}", taskName, func.getClass.getName, if (background) " in background" else "")
    val exec = CompletableFuture.supplyAsync(new Supplier[String]() {
      override def get(): String =  func(runVars)
    }, executorService).thenAccept(new Consumer[String]() {
      override def accept(result: String): Unit = {
        setTaskStatus(taskName, 'P', result)
      }
    }).exceptionally(new java.util.function.Function[Throwable, Void]() {
      override def apply(e: Throwable): Void = {
        e.printStackTrace()
        setTaskStatus(taskName, 'F', e.getMessage)
        logger.error("{} failed due to ", taskName, e: Any)
        null
      }
    })
    execCache.put(taskName, exec)
    if (!background) exec.join
  }

  private[fusion] def removeTask(taskName: String): Unit = {
    cancelTask(taskName, "Task removed by user")
    execCache.remove(taskName)
    funcCache.remove(taskName)
    taskStatusMap.remove(taskName)
  }

  private[fusion] def cancelTask(taskName: String, reason: String) = {
    val exec = execCache.get(taskName)
    if (exec == null || exec.isDone || exec.isCancelled || exec.isCompletedExceptionally) false
    else {
      taskStatusMap.put(taskName, 'C')
      logger.warn("Cancelling task {}", taskName)
      exec.cancel(true)
    }
  }

  def getAllTaskStatus: Seq[(String, Char)] = {
    taskMap.toSeq.map{case (taskName, _) =>
      (taskName, taskStatusMap.getOrElse(taskName, 'W'))
    }
  }

  def getTaskStatus(taskName: String): Char = taskStatusMap.getOrElse(taskName, 'W')

  private[fusion] def setTaskStatus(taskName: String, status:Char, reason:String): Boolean = {
    if (taskMap.containsKey(taskName)) {
      if (status == 'C' || status == 'P' || status == 'F')
        writeLog(taskName, status, reason)
      if (status == 'C') cancelTask(taskName, reason)
      else if (status == 'R') {
        if (taskStatusMap.getOrElse(taskName, 'W') == 'R') false else {
          taskStatusMap.put(taskName, 'R')
          runTask(taskName, true, vars)
          true
        }
      } else {
        taskStatusMap.put(taskName, status)
        logger.info("Changing task status of {} to {}", taskName, status)
        true
      }
    } else false
  }

  def cloneVars(): ConcurrentHashMap[String, String] = {
    new ConcurrentHashMap[String, String](vars)
  }

  def exportVars(sessionVars: java.util.Map[String, String], varNames:String*): Unit = {
    varNames.foreach(varName => sessionVars.get(varName) match {
        case null => vars.remove(varName)
        case aValue => vars.put(varName, aValue)
      })
  }

  private def writeLog(taskName:String, taskStatus:Char, message:String):Unit = {
    taskLog(taskLogIndex % 1000) = (System.currentTimeMillis, taskName, taskStatus, message)
    taskLogIndex += 1
  }

  def getLogs: Iterator[(Long, String, Char, String)] = {
    if (taskLogIndex > 1000) {
      val div = taskLogIndex % 1000
      val result = new Array[(Long, String, Char, String)](1000)
      System.arraycopy(taskLog, div, result, 0, 1000 - div)
      System.arraycopy(taskLog, 0, result, 1000 - div, div)
      taskLog.toIterator
    } else taskLog.toIterator.take(taskLogIndex)
  }

  def getLogSize:Int = if (taskLogIndex > 1000) 1000 else taskLogIndex
}
