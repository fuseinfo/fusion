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
package com.fuseinfo.spark.utils

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.hadoop.conf.Configuration

class SerializableConfiguration(conf: Configuration) extends Configuration(conf) with Externalizable {
  def this() {this(new Configuration(false))}

  override def readExternal(in: ObjectInput): Unit = {
    super.readFields(in)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    super.write(out)
  }
}
