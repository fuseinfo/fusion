package com.fuseinfo.fusion.ext

import com.fuseinfo.fusion.Fusion
import com.fuseinfo.fusion.util.VarUtils
import org.slf4j.LoggerFactory

import java.sql.DriverManager

class JdbcExec (params: Map[String, String]) extends (java.util.Map[String, String] => Boolean) {
  @transient private val logger = LoggerFactory.getLogger(this.getClass)
  private val reservedSet = Set("driver", "url", "sql")

  override def apply(stats: java.util.Map[String, String]): Boolean = try {
    val vars = Fusion.cloneVars()
    vars.putAll(stats)
    val enrichedParams = params.mapValues(VarUtils.enrichString(_, vars))
    enrichedParams.get("driver").foreach(Class.forName)
    val url = enrichedParams("url")
    val sql = enrichedParams("sql")
    val props = new java.util.Properties
    enrichedParams.filterKeys{key => !reservedSet.contains(key) && !key.startsWith("__")}
      .foreach(kv => props.setProperty(kv._1, kv._2))
    val conn = DriverManager.getConnection(url, props)
    val stmt = conn.createStatement()
    val result = stmt.execute(sql)
    stmt.close()
    conn.close()
    result
  } catch {
    case e: Exception =>
      logger.error("JdbcExec failed", e)
      false
  }

}