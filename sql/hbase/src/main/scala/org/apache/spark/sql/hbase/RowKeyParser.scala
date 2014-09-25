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

import java.util

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
trait RowKeyParser {
  def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq // .NavigableMap[String, HBaseRawType]

  def parseRowKeyWithMetaData(rowKey: HBaseRawType): Map[ColumnName, HBaseRawType]
}

case class RowKeySpec(offsets: Seq[Int])

case class CompositeRowKeyParser(rkCols: Seq[ColumnName]) extends RowKeyParser {

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

  override def parseRowKeyWithMetaData(rowKey: HBaseRawType): Map[ColumnName, HBaseRawType] = {
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
