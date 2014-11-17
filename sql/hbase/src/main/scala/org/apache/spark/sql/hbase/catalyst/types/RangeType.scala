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
package org.apache.spark.sql.hbase.catalyst.types

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.Utils

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.math.PartialOrdering
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, runtimeMirror, typeTag}

class Range[T](val start: Option[T], // None for open ends
               val startInclusive: Boolean,
               val end: Option[T], // None for open ends
               val endInclusive: Boolean,
               val dt:NativeType) {
  require(dt != null && !(start.isDefined && end.isDefined &&
    ((dt.ordering.eq(start.get, end.get) &&
      (!startInclusive || !endInclusive)) ||
      (dt.ordering.gt(start.get.asInstanceOf[dt.JvmType], end.get.asInstanceOf[dt.JvmType])))),
    "Inappropriate range parameters")
}

// HBase ranges:
// @param
// id: partition id to be used to map to a HBase partition
class PartitionRange[T](start: Option[T], startInclusive: Boolean,
                        end: Option[T], endInclusive: Boolean, val id: Int, dt:NativeType)
  extends Range[T](start, startInclusive, end, endInclusive, dt)

// A PointRange is a range of a single point. It is used for convenience when
// do comparison on two values of the same type. An alternatively would be to
// use multiple (overloaded) comparison methods, which could be more natural
// but also more codes

//class PointRange[T](value: T, dt:NativeType)
//  extends Range[T](Some(value), true, Some(value), true, dt)


class RangeType[T] extends PartiallyOrderingDataType {
  private[sql] type JvmType = Range[T]
  @transient private[sql] val tag = typeTag[JvmType]

  def toPartiallyOrderingDataType(s: Any, dt: NativeType): Any = s match {
    case i: Int => new Range[Int](Some(i), true, Some(i), true, IntegerType)
    case l: Long => new Range[Long](Some(l), true, Some(l), true, LongType)
    case d: Double => new Range[Double](Some(d), true, Some(d), true, DoubleType)
    case f: Float => new Range[Float](Some(f), true, Some(f), true, FloatType)
    case b: Byte => new Range[Byte](Some(b), true, Some(b), true, ByteType)
    case s: Short => new Range[Short](Some(s), true, Some(s), true, ShortType)
    case s: String => new Range[String](Some(s), true, Some(s), true, StringType)
    case b: Boolean => new Range[Boolean](Some(b), true, Some(b), true, BooleanType)
    case d: BigDecimal => new Range[BigDecimal](Some(d), true, Some(d), true, DecimalType)
    case t: Timestamp => new Range[Timestamp](Some(t), true, Some(t), true, TimestampType)
    case _ => s
  }

  val partialOrdering = new PartialOrdering[JvmType] {
    // Right now we just support comparisons between a range and a point
    // In the future when more generic range comparisons, these two methods
    // must be functional as expected
    def tryCompare(a: JvmType, b: JvmType): Option[Int] = {
      val p1 = lteq(a, b)
      val p2 = lteq(b, a)
      if (p1) {
        if (p2) Some(0) else Some(-1)
      } else if (p2) Some(1) else None
    }

    def lteq(a: JvmType, b: JvmType): Boolean = {
      // [(aStart, aEnd)] and [(bStart, bEnd)]
      // [( and )] mean the possibilities of the inclusive and exclusive condition
      val aRange = a.asInstanceOf[Range[T]]
      val aStartInclusive = aRange.startInclusive
      val aEnd = aRange.end.getOrElse(null)
      val aEndInclusive = aRange.endInclusive
      val bRange = b.asInstanceOf[Range[T]]
      val bStart = bRange.start.getOrElse(null)
      val bStartInclusive = bRange.startInclusive
      val bEndInclusive = bRange.endInclusive

      // Compare two ranges, return true iff the upper bound of the lower range is lteq to
      // the lower bound of the upper range. Because the exclusive boundary could be null, which
      // means the boundary could be infinity, we need to further check this conditions.
      val result =
        (aStartInclusive, aEndInclusive, bStartInclusive, bEndInclusive) match {
          // [(aStart, aEnd] compare to [bStart, bEnd)]
          case (_, true, true, _) => {
            if (aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
                true
            } else {
              false
            }
          }
          // [(aStart, aEnd] compare to (bStart, bEnd)]
          case (_, true, false, _) => {
            if (bStart != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          }
          // [(aStart, aEnd) compare to [bStart, bEnd)]
          case (_, false, true, _) => {
            if (aEnd != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          }
          // [(aStart, aEnd) compare to (bStart, bEnd)]
          case (_, false, false, _) => {
            if (aEnd != null && bStart != null &&
              aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
                bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          }
        }

      result
    }
  }
}

object RangeType {
  object StringRangeType extends RangeType[String]
  object IntegerRangeType extends RangeType[Int]
  object LongRangeType extends RangeType[Long]
  object DoubleRangeType extends RangeType[Double]
  object FloatRangeType extends RangeType[Float]
  object ByteRangeType extends RangeType[Byte]
  object ShortRangeType extends RangeType[Short]
  object BooleanRangeType extends RangeType[Boolean]
  object DecimalRangeType extends RangeType[BigDecimal]
  object TimestampRangeType extends RangeType[Timestamp]
  val primitiveToPODataTypeMap: HashMap[NativeType, PartiallyOrderingDataType] =
  HashMap(IntegerType->IntegerRangeType, LongType->LongRangeType, DoubleType->DoubleRangeType,
            FloatType->FloatRangeType, ByteType->ByteRangeType, ShortType->ShortRangeType,
            BooleanType->BooleanRangeType, DecimalType->DecimalRangeType,
            TimestampType->TimestampRangeType)
}
