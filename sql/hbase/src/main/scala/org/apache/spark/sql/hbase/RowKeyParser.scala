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

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructType}
import org.apache.spark.sql.hbase.HBaseCatalog.Columns

//case class RowKey(colVals: Seq[HColumn]) {
//  override def toString() = {
//    new String(RowKeyParser.parse
//  }
//}

/**
 * Trait for RowKeyParser's that convert a raw array of bytes into their constituent
 * logical column values
 *
 * Format of a RowKey is:
 * <version#><dim1-value><dim2-value>..<dimN-value>[offset1,offset2,..offset N]<# dimensions>
 * where:
 * #dimensions is an integer value represented in one byte. Max value = 255
 * each offset is represented by a short value in 2 bytes
 * each dimension value is contiguous, i.e there are no delimiters
 *
 * In short:
 * First: the VersionByte
 * Next: All of the Dimension Values (no delimiters between them)
 * Dimension Offsets: 16 bit values starting with 1 (the first byte after the VersionByte)
 * Last: DimensionCountByte
 *
 * example: 1HelloThere9999abcde<1><12><16>3
 * where
 *   1 = VersionByte
 *   HelloThere = Dimension1
 *   9999 = Dimension2
 *   abcde = Dimension3
 *   <1> = offset of Dimension1   <in 16-bits integer binary>
 *   <12> = offset of Dimension2   <in 16-bits integer binary>
 *   <16> = offset of Dimension3  <in 16-bits integer binary>
 *   3 = DimensionCountByte
 *
 * The rationale for putting the dimension values BEFORE the offsets and DimensionCountByte is to
 * facilitate RangeScan's for sequential dimension values.  We need the PREFIX of the key to be
 * consistent on the  initial bytes to enable the higher performance sequential scanning.
 * Therefore the variable parts - which include the dimension offsets and DimensionCountByte - are
 * placed at the end of the RowKey.
 *
 * We are assuming that a byte array representing the RowKey is completely filled by the key.
 * That is required for us to determine the length of the key
 * and retrieve the important DimensionCountByte.
 *
 * With the DimnensionCountByte the offsets can then be located and the values
 * of the Dimensions computed.
 *
 */
trait AbstractRowKeyParser {

  def createKey(rawBytes: HBaseRawRowSeq, version: Byte): HBaseRawType

  def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq // .NavigableMap[String, HBaseRawType]

  def parseRowKeyWithMetaData(rkCols: Seq[ColumnName], rowKey: HBaseRawType)
  : Map[ColumnName, HBaseRawType]
}

case class RowKeySpec(offsets: Seq[Int], version: Byte = RowKeyParser.Version1)

object RowKeyParser extends AbstractRowKeyParser {

  val Version1 = '1'.toByte

  val VersionFieldLen = 1
  // Length in bytes of the RowKey version field
  val DimensionCountLen = 1
  // One byte for the number of key dimensions
  val MaxDimensions = 255
  val OffsetFieldLen = 2

  // Two bytes for the value of each dimension offset.
  // Therefore max size of rowkey is 65535.  Note: if longer rowkeys desired in future
  // then simply define a new RowKey version to support it. Otherwise would be wasteful
  // to define as 4 bytes now.
  def computeLength(keys: HBaseRawRowSeq) = {
    VersionFieldLen + keys.map {
      _.length
    }.sum + OffsetFieldLen * keys.size + DimensionCountLen
  }

  override def createKey(keys: HBaseRawRowSeq, version: Byte = Version1): HBaseRawType = {
    var barr = new Array[Byte](computeLength(keys))
    val arrayx = new AtomicInteger(0)
    barr(arrayx.getAndAdd(VersionFieldLen)) = version // VersionByte
    val valuesStartIndex = new AtomicInteger(arrayx.get)
    keys.foreach { k => copyToArr(barr, k, arrayx.getAndAdd(k.length))} // Dim values
    keys.foreach { k => // Offsets
      copyToArr(barr,
        short2b(valuesStartIndex.getAndAdd(k.length).toShort),
        arrayx.addAndGet(OffsetFieldLen))
    }
    barr(arrayx.get) = keys.length.toByte // DimensionCountByte
    barr
  }

  def copyToArr[T](a: Array[T], b: Array[T], aoffset: Int) = {
    //    System.arraycopy(a,aoffset,b,0,b.length)
    b.copyToArray(a, aoffset)
  }

  def short2b(sh: Short): Array[Byte] = {
    val barr = Array[Byte](2)
    barr(0) = ((sh >> 8) & 0xff).toByte
    barr(1) = (sh & 0xff).toByte
    barr
  }

  def b2Short(barr: Array[Byte]) = {
    (barr(0).toShort << 8) | barr(1).toShort
  }

  def createKeyFromCatalystRow(schema: StructType, keyCols: Columns, row: Row) = {
    // TODO(sboesch): provide proper data-type specific serde's.
    // For now just use to/from String
    val rawKeyCols = CatalystToHBase.catalystRowToHBaseRawVals(schema, row, keyCols)
    createKey(rawKeyCols)
  }

  def getMinimumRowKeyLength = VersionFieldLen + DimensionCountLen

  override def parseRowKey(rowKey: HBaseRawType): HBaseRawRowSeq = {

    assert(rowKey.length >= getMinimumRowKeyLength,
      s"RowKey is invalid format - less than minlen . Actual length=${rowKey.length}")
    assert(rowKey(0).toByte == Version1, s"Only Version1 supported. Actual=${rowKey(0).toByte}")
    val ndims: Int = b2Short(rowKey.slice(rowKey.length - DimensionCountLen - 1, rowKey.length))
    val offsetsStart = rowKey.length - DimensionCountLen - ndims * OffsetFieldLen - 1
    val rowKeySpec = RowKeySpec(
      for (dx <- 0 to ndims)
      yield b2Short(rowKey.slice(offsetsStart + dx * OffsetFieldLen,
        offsetsStart + dx * (OffsetFieldLen + 1) + 1))
    )

    val endOffsets = rowKeySpec.offsets.tail :+ (rowKey.length - DimensionCountLen - 1)
    val colsList = rowKeySpec.offsets.zipWithIndex.map { case (off, ix) =>
      rowKey.slice(off, endOffsets(ix)).asInstanceOf[HBaseRawType]
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
