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

import java.util.concurrent.atomic.AtomicInteger

case class RowKey(colVals: Seq[HColumn])

/**
 * Trait for RowKeyParser's that convert a raw array of bytes into their constituent
 * logical column values
 *
 * Format of a RowKey is:
 * <# dimensions>[offset1,offset2,..offset N]<dim1-value><dim2-value>..<dimN-value>
 * where:
 * #dimensions is an integer value represented in one byte. Max value = 255
 * each offset is represented by a short value in 2 bytes
 * each dimension value is contiguous, i.e there are no delimiters
 *
 */
trait AbstractRowKeyParser {
  def createKey(rawBytes : HBaseRawRowSeq, version : Byte) : HBaseRawType

  def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq // .NavigableMap[String, HBaseRawType]

  def parseRowKeyWithMetaData(rkCols: Seq[ColumnName], rowKey: HBaseRawType)
        : Map[ColumnName, HBaseRawType]
}

case class RowKeySpec(offsets: Seq[Int], version : Byte = 1)

object RowKeyParser extends AbstractRowKeyParser {

  val VersionFieldLen = 1  // Length in bytes of the RowKey version field
  val LenFieldLen = 1  // One byte for the number of key dimensions
  val MaxDimensions = 255
  val OffsetFieldLen = 2   // Two bytes for the value of each dimension offset.

  // Therefore max size of rowkey is 65535.  Note: if longer rowkeys desired in future
  // then simply define a new RowKey version to support it. Otherwise would be wasteful
  // to define as 4 bytes now.
  def computeLength(keys: HBaseRawRowSeq) = {
    VersionFieldLen + LenFieldLen + OffsetFieldLen * keys.size + keys.map{_.length}.sum
  }
  def copyToArr[T](a : Array[T], b : Array[T], aoffset : Int) = {
//    System.arraycopy(a,aoffset,b,0,b.length)
    b.copyToArray(a,aoffset)
  }

  override def createKey(keys: HBaseRawRowSeq, version : Byte = 1): HBaseRawType = {
    var barr = new Array[Byte](computeLength(keys))
    barr(0) = 1.toByte
    barr(0) = keys.length.toByte
    val ax = new AtomicInteger(VersionFieldLen + LenFieldLen)
    keys.foreach{ k => copyToArr(barr, k, ax.addAndGet(OffsetFieldLen)) }
    keys.foreach{ k => copyToArr(barr, k, ax.addAndGet(k.length)) }
    barr
  }

  override def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq = {

    val ndims: Int = rowKey(0).toInt
    val rowKeySpec = RowKeySpec(
      for (dx <- 0 to ndims)
      yield new String(rowKey.slice(1 + dx * 2, 1 + 2 + dx * 2)).toInt
    )

    val endOffsets = rowKeySpec.offsets.tail :+ Int.MaxValue
    val colsList = rowKeySpec.offsets.zipWithIndex.map { case (o, ix) =>
      rowKey.slice(o, endOffsets(ix)).asInstanceOf[HBaseRawType]
    }
  }.asInstanceOf[HBaseRawRowSeq]

  override def parseRowKeyWithMetaData(rkCols: Seq[ColumnName], rowKey: HBaseRawType):
  Map[ColumnName, HBaseRawType] = {
    import scala.collection.mutable.HashMap

    val rowKeyVals = parseRowKey(rowKey)
    val rmap = rowKeyVals.zipWithIndex.foldLeft(new HashMap[ColumnName, HBaseRawType]()) {
      case (m, (cval, ix)) =>
        m.update(rkCols(ix), cval)
        m
      }
    rmap.toMap[ColumnName, HBaseRawType]
  }

}
