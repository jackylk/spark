package org.apache.spark.sql.hbase

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StructType, DoubleType, StringType, ShortType}
import org.apache.spark.sql.hbase.HBaseCatalog.{Columns, Column}
import org.apache.spark.sql.hbase.RowKeyParser._
import org.scalatest.{ShouldMatchers, FunSuite}
import DataTypeUtils._

/**
 * CompositeRowKeyParserTest
 * Created by sboesch on 9/25/14.
 */
class RowKeyParserSuite extends FunSuite with ShouldMatchers {
  @transient val logger = Logger.getLogger(getClass.getName)

  def makeRowKey(col7: Double, col1: String, col3: Short) = {
    val size = 1 + sizeOf(col7) + sizeOf(col1) + sizeOf(col3) + 3 * 2 + DimensionCountLen
    //      val barr = new Array[Byte](size)
    val bos = new ByteArrayOutputStream(size)
    val dos = new DataOutputStream(bos)
    dos.writeByte(RowKeyParser.Version1)
    dos.writeDouble(col7)
    dos.writeBytes(col1)
    dos.writeShort(col3)
    var off = 1
    dos.writeShort(off)
    off += sizeOf(col7)
    dos.writeShort(off)
    off += sizeOf(col1)
    dos.writeShort(off)
    dos.writeByte(3.toByte)
    val s = bos.toString
    //      println((s"MakeRowKey: [${RowKeyParser.show(bos.toByteArray)}]")
    println(s"MakeRowKey: [${s}]")
    bos.toByteArray
  }

  test("rowkey test") {

    val cols = Range(0, 3).zip(Seq(DoubleType, StringType, ShortType))
      .map { case (ix, dataType) =>
        Column(s"col{ix+10}",s"cf${ix + 1}", s"cq${ix + 10}", dataType)
    }.toSeq

    val pat = makeRowKey(12345.6789, "Column1-val",12345)
    val parsedKeyMap = RowKeyParser.parseRowKeyWithMetaData(cols, pat)
    println(s"parsedKeyWithMetaData: ${parsedKeyMap.toString}")
    assert(parsedKeyMap === Map("col7" -> (12345.6789, "col1" -> "Column1-val","col3" ->12345)))
//    assert(parsedKeyMap.values.toList.sorted === List(12345.6789, "Column1-val",12345))

    val parsedKey = RowKeyParser.parseRowKey(pat)
    println(s"parsedRowKey: ${parsedKey.toString}")

  }

  test("CreateKeyFromCatalystRow") {
    def createKeyFromCatalystRow(schema: StructType, keyCols: Columns, row: Row) = {
      // TODO(sboesch): provide proper data-type specific serde's.
      // For now just use to/from String
      val rawKeyCols = CatalystToHBase.catalystRowToHBaseRawVals(schema, row, keyCols)
      createKey(rawKeyCols)
    }
  }

}
