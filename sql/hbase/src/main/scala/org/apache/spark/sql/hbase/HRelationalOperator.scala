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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.log4j.Logger

/**
 * RelationalOperator
 * Created by sboesch on 9/24/14.
 */
sealed trait HRelationalOperator {
  def toHBase: CompareOp
  def cmp(col1: Array[Byte] /* ByteArrayComparable */,
          col2 : Array[Byte] /*ByteArrayComparable */) : Boolean
  def compare(col1: Array[Byte], col2: Array[Byte]) = {
    if (col1 == null || col2 == null) {
      throw new IllegalArgumentException("RelationalOperator: Can not compare nulls")
    } else {
      new String(col1).compareTo(new String(col2))
      // TODO(sboesch): do proper byte array comparison
      //      val c1len = col1.length
      //      val c2len = col2.length
      //      if (c1len == 0 && c2len == 0) {
      //        0
      //      } else {
      //        var c1ptr = 0
      //        var c2ptr = 0
      //        import scala.util.control.Breaks._
      //        breakable {
      //          while (c1ptr < c1len && c2ptr < c2len) {
      //            if (col1(c1ptr) <= col2(c2ptr)) {
      //              c1ptr+=1
      //            } else {
      //              c2ptr+=1
      //            }
      //          }
      //          if (c1ptr < c1len
      //
      //        }
    }
  }

}

case object LT extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.LESS
  }

  override def cmp(col1: Array[Byte] /* ByteArrayComparable */,
                   col2 : Array[Byte] /*ByteArrayComparable */) = compare(col1,col2) < 0
}

case object LTE extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.LESS_OR_EQUAL
  }
  override def cmp(col1: Array[Byte] /* ByteArrayComparable */,
                   col2 : Array[Byte] /*ByteArrayComparable */) = compare(col1,col2) <= 0
}

case object EQ extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.EQUAL
  }
  override def cmp(col1: Array[Byte] /* ByteArrayComparable */,
                   col2 : Array[Byte] /*ByteArrayComparable */) = compare(col1,col2) == 0
}

case object GTE extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.GREATER_OR_EQUAL
  }
  override def cmp(col1: Array[Byte] /* ByteArrayComparable */,
                   col2 : Array[Byte] /*ByteArrayComparable */) = compare(col1,col2) >= 0
}

case object GT extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.GREATER
  }
  override def cmp(col1: Array[Byte] /* ByteArrayComparable */,
                   col2 : Array[Byte] /*ByteArrayComparable */) = compare(col1,col2) > 0
}
