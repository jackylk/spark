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

import org.apache.spark.sql.catalyst.expressions._

/**
 * find the critical points in the given expression
 */
case class HBaseCriticalPointsFinder(input: Row, keyColumns: Seq[KeyColumn]) {
  var pointSet = Set[Literal]()

  def findPoints(e: Expression): Unit = {
    e match {
      case LessThan(left, right) => {
        extract(left, right)
      }
      case EqualTo(left, right) => {
        extract(left, right)
      }
      case LessThan(left, right) => {
        extract(left, right)
      }
      case LessThanOrEqual(left, right) => {
        extract(left, right)
      }
      case GreaterThan(left, right) => {
        extract(left, right)
      }
      case GreaterThanOrEqual(left, right) => {
       extract(left, right)
      }
      case And(left, right)  => {
        findPoints(left)
        findPoints(right)
      }
      case Or(left, right)  => {
        findPoints(left)
        findPoints(right)
      }
      case Not(child)  => {
        findPoints(child)
      }
    }
  }

  def extract(left: Expression, right: Expression) = {
    if (left.isInstanceOf[Literal]) {
      pointSet = pointSet + left.asInstanceOf[Literal]
    } else if (right.isInstanceOf[Literal]) {
      pointSet = pointSet + right.asInstanceOf[Literal]
    }
  }
}
