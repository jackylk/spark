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

import scala.collection.mutable.Set
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{NativeType, DataType}
import org.apache.spark.sql.hbase.CriticalPointType.CriticalPointType

object CriticalPointType extends Enumeration {
  type CriticalPointType = Value
  val upInclusive = Value("Up Inclusive: (...)[...)")
  val lowInclusive = Value("Low Inclusive: (...](...)")
  val bothInclusive = Value("Both Inclusive: (...)[](...)")
}

case class CriticalPoint[T](value: T, ctype: CriticalPointType, dt: DataType) {
  override def hashCode() = value.hashCode()
  override def equals(other: Any): Boolean = other match {
    case cp: CriticalPoint[T] => value.equals(cp.value)
    case _ => false
  }
}

/**
 * find the critical points in the given expressiona: not really a transformer
 * Must be called before reference binding
 */
object RangeCriticalPointsFinder {
  def apply(expression: Expression, key: AttributeReference): Set[CriticalPoint[_]] = {
    val pointSet = Set[CriticalPoint[_]]()
    val dt: NativeType = expression.dataType.asInstanceOf[NativeType]
    type JvmType = dt.JvmType
    def checkAndAdd(value: Any, ct: CriticalPointType): Unit = {
      val cp = CriticalPoint[JvmType](value.asInstanceOf[JvmType], ct, dt)
      if (!pointSet.add(cp)) {
        val oldCp = pointSet.find(_.value==value).get
        if (oldCp.ctype != ct && oldCp.ctype != CriticalPointType.bothInclusive) {
          pointSet.remove(cp)
          if (ct == CriticalPointType.bothInclusive) {
            pointSet.add(cp)
          } else {
            pointSet.add(CriticalPoint[JvmType](value.asInstanceOf[JvmType],
                         CriticalPointType.bothInclusive, dt))
          }
        }
      }
    }
    expression transform {
      case a@EqualTo(AttributeReference(_,_,_), Literal(value, _)) => {
        if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
        a
      }
      case a@EqualTo(Literal(value, _), AttributeReference(_,_,_)) => {
        if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
        a
      }
      case a@LessThan(AttributeReference(_,_,_), Literal(value, _)) => {
        if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
        a
      }
      case a@LessThan(Literal(value, _), AttributeReference(_,_,_)) => {
        if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
        a
      }
      case a@LessThanOrEqual(AttributeReference(_,_,_), Literal(value, _)) => {
        if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
        a
      }
      case a@LessThanOrEqual(Literal(value, _), AttributeReference(_,_,_)) => {
        if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
        a
      }
      case a@GreaterThanOrEqual(AttributeReference(_,_,_), Literal(value, _)) => {
        if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
        a
      }
      case a@GreaterThanOrEqual(Literal(value, _), AttributeReference(_,_,_)) => {
        if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
        a
      }
    }
    pointSet
  }
}
