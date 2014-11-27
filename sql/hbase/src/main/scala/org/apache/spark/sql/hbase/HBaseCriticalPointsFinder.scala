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

import org.apache.spark.sql.hbase.catalyst.types.PartitionRange

import scala.collection.mutable.{ArrayBuffer, Set}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{IntegralType, NativeType}
import org.apache.spark.sql.hbase.CriticalPointType.CriticalPointType

object CriticalPointType extends Enumeration {
  type CriticalPointType = Value
  val upInclusive = Value("Up Inclusive: (...)[...)")
  val lowInclusive = Value("Low Inclusive: (...](...)")
  val bothInclusive = Value("Both Inclusive: (...)[](...)")
}

case class CriticalPoint[T](value: T, ctype: CriticalPointType, dt: NativeType) {
  override def hashCode() = value.hashCode()
  val decreteType: Boolean = dt.isInstanceOf[IntegralType]
  override def equals(other: Any): Boolean = other match {
    case cp: CriticalPoint[T] => value.equals(cp.value)
    case _ => false
  }
}

/**
 * find the critical points in the given expressiona: not really a transformer
 * Must be called before reference binding
 */
object RangeCriticalPoint {
  def collect[T](expression: Expression, key: AttributeReference): Seq[CriticalPoint[T]] = {
    if (key.references.subsetOf(expression.references)) {
      val pointSet = Set[CriticalPoint[T]]()
      val dt: NativeType = expression.dataType.asInstanceOf[NativeType]
      def checkAndAdd(value: Any, ct: CriticalPointType): Unit = {
        val cp = CriticalPoint[T](value.asInstanceOf[T], ct, dt)
        if (!pointSet.add(cp)) {
          val oldCp = pointSet.find(_.value == value).get
          if (oldCp.ctype != ct && oldCp.ctype != CriticalPointType.bothInclusive) {
            pointSet.remove(cp)
            if (ct == CriticalPointType.bothInclusive) {
              pointSet.add(cp)
            } else {
              pointSet.add(CriticalPoint[T](value.asInstanceOf[T],
                CriticalPointType.bothInclusive, dt))
            }
          }
        }
      }
      expression transform {
        case a@EqualTo(AttributeReference(_, _, _), Literal(value, _)) => {
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        }
        case a@EqualTo(Literal(value, _), AttributeReference(_, _, _)) => {
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        }
        case a@LessThan(AttributeReference(_, _, _), Literal(value, _)) => {
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        }
        case a@LessThan(Literal(value, _), AttributeReference(_, _, _)) => {
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        }
        case a@LessThanOrEqual(AttributeReference(_, _, _), Literal(value, _)) => {
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        }
        case a@LessThanOrEqual(Literal(value, _), AttributeReference(_, _, _)) => {
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        }
        case a@GreaterThanOrEqual(AttributeReference(_, _, _), Literal(value, _)) => {
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        }
        case a@GreaterThanOrEqual(Literal(value, _), AttributeReference(_, _, _)) => {
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        }
      }
      pointSet.toSeq.sortWith((a: CriticalPoint[T], b: CriticalPoint[T])
      => dt.ordering.lt(a.value.asInstanceOf[dt.JvmType], b.value.asInstanceOf[dt.JvmType]))
    } else Nil
  }
/*
 * create partition ranges on a *sorted* list of critical points
 */
  def generatePartitionRange[T](cps: Seq[CriticalPoint[T]], dt: NativeType)
         : Seq[PartitionRange[T]] = {
    if (cps.isEmpty) Nil
    else {
      val discreteType = dt.isInstanceOf[IntegralType]
      val result = new ArrayBuffer[PartitionRange[T]](cps.size + 1)
      var prev: CriticalPoint[T] = null
      cps.foreach(cp=> {
        if (prev == null) {
          cp.ctype match {
            case CriticalPointType.lowInclusive =>
              result += new PartitionRange[T](None, false, Some(cp.value), true, -1, cp.dt, null)
            case CriticalPointType.upInclusive =>
              result += new PartitionRange[T](None, false, Some(cp.value), false, -1, cp.dt, null)
            case CriticalPointType.bothInclusive =>
              result += (new PartitionRange[T](None, false, Some(cp.value), false, -1, cp.dt, null),
                new PartitionRange[T](Some(cp.value), true, Some(cp.value), true, -1, cp.dt, null))
          }
        } else {
          (prev.ctype, cp.ctype) match {
            case (CriticalPointType.lowInclusive, CriticalPointType.lowInclusive) =>
              result += new PartitionRange[T](Some(prev.value), false,
                                              Some(cp.value), true, -1, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.upInclusive) =>
              result += new PartitionRange[T](Some(prev.value), false,
                                              Some(cp.value), false, -1, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.bothInclusive) =>
              result += (new PartitionRange[T](Some(prev.value), false,
                Some(cp.value), false, -1, cp.dt, null),
                new PartitionRange[T](Some(cp.value), true,
                  Some(cp.value), true, -1, cp.dt, null))
            case (CriticalPointType.upInclusive, CriticalPointType.lowInclusive) =>
              result += new PartitionRange[T](Some(prev.value), true,
                                              Some(cp.value), true, -1, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.upInclusive) =>
              result += new PartitionRange[T](Some(prev.value), true,
                                              Some(cp.value), false, -1, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.bothInclusive) =>
              result += (new PartitionRange[T](Some(prev.value), true,
                Some(cp.value), false, -1, cp.dt, null),
                new PartitionRange[T](Some(cp.value), true,
                  Some(cp.value), true, -1, cp.dt, null))
            case (CriticalPointType.bothInclusive, CriticalPointType.lowInclusive) =>
              result += new PartitionRange[T](Some(prev.value), false,
                                              Some(cp.value), true, -1, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.upInclusive) =>
              result += new PartitionRange[T](Some(prev.value), false,
                                              Some(cp.value), false, -1, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.bothInclusive) =>
              result += (new PartitionRange[T](Some(prev.value), false,
                Some(cp.value), false, -1, cp.dt, null),
                new PartitionRange[T](Some(cp.value), true,
                  Some(cp.value), true, -1, cp.dt, null))
          }
        }
        prev = cp
      })
      if (prev != null) {
        result += {
          prev.ctype match {
            case CriticalPointType.lowInclusive =>
              new PartitionRange[T](Some(prev.value), false, None, false, -1, prev.dt, null)
            case CriticalPointType.upInclusive =>
              new PartitionRange[T](Some(prev.value), true, None, false, -1, prev.dt, null)
            case CriticalPointType.bothInclusive =>
              new PartitionRange[T](Some(prev.value), false, None, false, -1, prev.dt, null)
          }
        }
      }
      // remove any redundant ranges for integral type
      if (discreteType) {
        var prev: PartitionRange[T] = null
        var prevChanged = false
        var thisChangedUp = false
        var thisChangedDown = false
        var newRange: PartitionRange[T] = null
        val newResult = new ArrayBuffer[PartitionRange[T]](result.size)
        result.foreach(r=>{
          thisChangedDown = false
          thisChangedUp = false
          if (r.startInclusive && !r.endInclusive && r.end.isDefined
               && r.start.get==
                  dt.ordering.asInstanceOf[Integral[T]].minus(r.end.get, 1.asInstanceOf[T])) {
            thisChangedDown = true
            if (prev != null && prev.startInclusive && prev.endInclusive
                && prev.start.get == prev.end.get && prev.start.get == r.start.get)
            {
              // the previous range is a equivalent point range => merge it with current one
              newRange = null
            } else {
              newRange = new PartitionRange[T](r.start, true, r.start, true, -1, r.dt, null)
            }
          } else if (!r.startInclusive && r.endInclusive && r.end.isDefined
              && r.start.get==
                 dt.ordering.asInstanceOf[Integral[T]].minus(r.end.get, 1.asInstanceOf[T])) {
            newRange = new PartitionRange[T](r.end, true, r.end, true, -1, r.dt, null)
            thisChangedUp = true
          } else newRange = r

          // the previous range has been changed up and this one has not changed =>
          // check whether this is mergeable with the (changed) previous
          if (newRange != null && !thisChangedDown && !thisChangedUp && prevChanged) {
            if (r.startInclusive && r.endInclusive && r.start.get == r.end.get &&
                prev.startInclusive && prev.endInclusive
                && prev.start.get == prev.end.get && prev.start.get == r.start.get) {
              newRange = null // merged with the previous range
            }
          }
          if (newRange != null) {
            newResult += newRange
            prev = newRange
            prevChanged = thisChangedUp
          }
        })
        newResult
      } else result
    }
  }
}
