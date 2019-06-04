/*
 * Copyright 2018 Fuseinfo Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fuseinfo.fusion.spark

import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.scp.ScpCommandFactory
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory

trait SftpBase {
  scala.util.Try(SftpBase.sshd.start())
}

object SftpBase {
  val sshd:SshServer = SshServer.setUpDefaultServer()
  sshd.setPort(14022)
  sshd.setSubsystemFactories(java.util.Arrays.asList(new SftpSubsystemFactory))
  sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider)
  sshd.setCommandFactory(new ScpCommandFactory)
  //  sshd.setShellFactory(new ProcessShellFactory)
  sshd.setPasswordAuthenticator(new PasswordAuthenticator(){
    override def authenticate(username: String, password: String, session: ServerSession): Boolean =
      username == password
  })
}