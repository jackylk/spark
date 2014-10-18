/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase

import org.apache.spark.sql.DataType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{IntegerType, LongType, StringType}

case class ColumnName(var family: Option[String], qualifier: String) {
  if (family.isDefined && family.get == null) {
    family = None
  }

  override def toString = fullName

  def fullName = if (family.isDefined) {
    s"$family:$qualifier"
  } else {
    s":$qualifier"
  }

  //  override def equals(other: Any) = {
  //    if (!other.isInstanceOf[ColumnName]) {
  //      false
  //    }
  //    val cother = other.asInstanceOf[ColumnName]
  //    family == cother.family && qualifier == cother.qualifier
  //  }
}

object ColumnName {
  def apply(compoundStr: String) = {
    val toks = compoundStr.split(":").toList
    if (toks.size == 2) {
      new ColumnName(Some(toks(0)), toks(1))
    } else {
      new ColumnName(None, toks(0))
    }
  }
}
