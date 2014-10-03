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
package org.apache.spark.sql

import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericMutableRow}

import scala.language.implicitConversions
/**
 * package
 * Created by sboesch on 9/22/14.
 */
package object hbase {

  type HBaseRawType = Array[Byte]
  type HBaseRawRow = Array[HBaseRawType]
  type HBaseRawRowSeq = Seq[HBaseRawType]

  class HBaseRow(vals : HBaseRawRow) extends GenericRow(vals.asInstanceOf[Array[Any]])

  val HBaseByteEncoding = "ISO-8859-1"
  def s2b(str: String) = str.getBytes(HBaseByteEncoding)

  class Optionable[T <: AnyRef](value: T) {
    def opt: Option[T] = if ( value == null ) None else Some(value)
  }

  implicit def anyRefToOptionable[T <: AnyRef](value: T) = new Optionable(value)

  case class SerializableTableName(@transient inTableName : TableName) {
    val namespace = inTableName.getNamespace
    val name = inTableName.getName
    @transient lazy val tableName : TableName = TableName.valueOf(namespace, name)
  }

  def binarySearchLowerBound[T, U](xs: IndexedSeq[T], key: U, keyExtract:
  (T) => U = (x:T) => x)(implicit ordering: Ordering[U]): Option[Int] = {
    var len = xs.length
    var first = 0
    var found = false
    while (!found && len > 0) {
      val half = len >>> 1
      val middle = first + half
      val arrval = keyExtract(xs(middle))
      if (ordering.eq(arrval, key)) {
        first = middle
        found = true
      } else if (ordering.lt(arrval, key)) {
        first = middle + 1
        len = len - half - 1
      } else {
        len = half
      }
    }
    if (first < xs.length)
      Some(first)
    else
      None
  }

}
