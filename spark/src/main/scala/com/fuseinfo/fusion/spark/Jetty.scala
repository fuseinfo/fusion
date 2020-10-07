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
package com.fuseinfo.fusion.spark

import java.io.IOException
import java.security.Principal

import com.fuseinfo.fusion.util.{ClassUtils, VarUtils}
import javax.naming.Context
import javax.naming.directory.{InitialDirContext, SearchControls}
import org.mortbay.jetty.bio.SocketConnector
import org.mortbay.jetty.{Request, Server}
import org.mortbay.jetty.handler.{AbstractHandler, ContextHandler, HandlerList, ResourceHandler}
import org.mortbay.jetty.security._

import scala.collection.JavaConversions._

class Jetty(taskName:String, params:java.util.Map[String, AnyRef])
  extends (java.util.Map[String, String] => String) with Serializable {

  def this(taskName:String) = this(taskName, new java.util.HashMap[String, AnyRef])

  override def apply(vars: java.util.Map[String, String]): String = {

    val enrichedParams = params.filter(_._2.isInstanceOf[String])
      .mapValues(v => VarUtils.enrichString(v.toString, vars))
    params.get("trustStore") match {
      case store:String => System.setProperty("javax.net.ssl.trustStore", store)
      case _ =>
    }
    val defaultPort = params.get("port") match {
      case num:Any if num.toString.matches("\\d+") => Some(num.toString.toInt)
      case _ => None
    }
    val connector = params.get("keyStore") match {
      case file:String =>
        val connector = new SslSocketConnector

        val keyPass = params.get("keyStorePassword")
        connector.setKeystore(file)
        connector.setKeyPassword(keyPass.toString)
        connector.setPort(defaultPort.getOrElse(14443))
        connector
      case _ =>
        val connector = new SocketConnector
        connector.setPort(defaultPort.getOrElse(14080))
        connector
    }

    val staticHandler = new ResourceHandler
    staticHandler.setResourceBase(getClass.getClassLoader.getResource("static").toExternalForm)

    val handlerList = new HandlerList
    val mappings = ClassUtils.getAllClasses(null, classOf[FusionHandler]).map{case (name, clazz) =>
      val context = new ContextHandler
      val handler = clazz.newInstance().asInstanceOf[FusionHandler]
      context.addHandler(handler)
      context.setContextPath(handler.getContext)
      handlerList.addHandler(context)
      val roles = handler.getRoles
      if (roles != null && roles.length > 0) {
        val constraintMapping = new ConstraintMapping
        constraintMapping.setPathSpec(handler.getContext + "/*")
        val constraint = new Constraint
        constraint.setAuthenticate(true)
        constraint.setName(name)
        constraint.setRoles(roles)
        constraintMapping.setConstraint(constraint)
        constraintMapping
      } else null
    }.filter(_ != null)

    val handler = enrichedParams.get("login") match {
      case Some("ldap") =>
        val securityHandler = new SecurityHandler
        val userRealm = new LdapUserRealm(enrichedParams.filterKeys(_.startsWith("ldap.")).toMap)
        securityHandler.setAuthMethod(Constraint.__BASIC_AUTH)
        val authenticator = new BasicAuthenticator
        securityHandler.setAuthenticator(authenticator)
        securityHandler.setUserRealm(userRealm)
        securityHandler.setHandler(handlerList)
        securityHandler.setConstraintMappings(mappings.toArray)
        securityHandler
      case Some("file") =>
        val securityHandler = new SecurityHandler
        val userRealm = new HashUserRealm()
        userRealm.put("admin", "admin")
        userRealm.addUserToRole("admin", "admin")
        securityHandler.setAuthMethod(Constraint.__BASIC_AUTH)
        val authenticator = new BasicAuthenticator
        securityHandler.setAuthenticator(authenticator)
        securityHandler.setUserRealm(userRealm)
        securityHandler.setHandler(handlerList)
        securityHandler.setConstraintMappings(mappings.toArray)
        securityHandler
      case _ => handlerList
    }

    val server = new Server
    server.setHandlers(Array(staticHandler, handler))
    try {
      connector.open()
    } catch{
      case _:IOException =>
        connector.setPort(0)
        connector.open()
    }
    server.addConnector(connector)
    server.start()

    "Started Jetty UI"
  }

  def getProcessorSchema:String = """{"title": "Jetty","type": "object","properties": {
    "__class":{"type":"string","options":{"hidden":true},"default":"spark.Jetty"},
    "login":{"type":"string","description":"Login type: ldap, file"},
    "trustStore":{"type":"string","description":"javax.net.ssl.trustStore"},
    "keyStore":{"type":"string","description":"javax.net.ssl.keyStore"},
    "keyStorePassword":{"type":"string","description":"javax.net.ssl.keyStorePassword"}
    },"required":["__class"]}"""

class LdapUserRealm(props: Map[String, String]) extends UserRealm {

 private val mappings = props.filterKeys(_.startsWith("ldap.group.")).map{kv =>
   kv._1.substring(11) -> kv._2.split(",").map(_.trim).toSet
 }

 override def getName: String = "LdapUserRealm"

 override def getPrincipal(username: String): Principal = new Principal {
   override def getName: String = username
 }

 override def authenticate(username: String, credentials: Any, request: Request): Principal = {
   val env = new java.util.Hashtable[String, String]
   try {
     env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
     env.put(Context.SECURITY_AUTHENTICATION, "Simple")
     env.put("com.sun.jndi.ldap.connect.pool", "true")
     env.put(Context.PROVIDER_URL, props("ldap.provider.url"))
     env.put(Context.SECURITY_PRINCIPAL, username)
     env.put(Context.SECURITY_CREDENTIALS, credentials.toString)
     val context = new InitialDirContext(env)
     val filter = String.format("(sAMAccountName=%s)", username.substring(username.indexOf('\\') + 1))
     val cons = new SearchControls
     cons.setSearchScope(SearchControls.SUBTREE_SCOPE)
     cons.setReturningAttributes(Array[String]("memberOf"))
     val baseName = props.getOrElse("ldap.base.dc", "")
     val results = context.search(baseName, filter, cons).nextElement().getAttributes
     val memberOf = results.get("memberOf").getAll
     val roles = memberOf.map { item =>
       val str = item.toString
       val len = str.length
       var i = str.indexOf("CN=") + 3
       val sb = new StringBuilder
       while (i < len) {
         val c = str.charAt(i)
         if (c == '\\' && i < len - 1) {
           i += 1
           sb.append(str.charAt(i))
         } else if (c == ',') {
           i = len
         } else {
           sb.append(c)
         }
         i += 1
       }
       mappings.getOrElse(sb.toString, Set.empty[String])
     }.reduceLeft(_ ++ _)
     new KnownUser(username, roles)
   } catch {
     case _:Exception => null
   }
 }

 override def reauthenticate(user: Principal): Boolean = user.isInstanceOf[KnownUser]

 override def isUserInRole(user: Principal, role: String): Boolean = user match {
   case knownUser: KnownUser => knownUser.roles.contains(role)
   case _ => false
 }

 override def disassociate(user: Principal): Unit = {}

 override def pushRole(user: Principal, role: String): Principal = new WrappedUser(user, role)

 override def popRole(user: Principal): Principal = user match {
   case wrappedUser: WrappedUser => wrappedUser.principal
   case _ => user
 }

 override def logout(user: Principal): Unit = {}
}

class KnownUser(userName:String, val roles:Set[String]) extends Principal {
 override def getName: String = userName
}

class WrappedUser(val principal: Principal, role: String) extends KnownUser(principal.getName, principal match {
 case user: KnownUser => user.roles + role
 case _ => Set(role)
})
}

trait FusionHandler extends AbstractHandler {
def getContext:String

def getRoles:Array[String]
}