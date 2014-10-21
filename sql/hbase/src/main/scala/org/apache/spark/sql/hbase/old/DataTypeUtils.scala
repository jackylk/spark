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

import java.io.{DataOutputStream, ByteArrayOutputStream, DataInputStream, ByteArrayInputStream}
import java.math.BigDecimal

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._

/**
 * DataTypeUtils
 * Created by sboesch on 10/9/14.
 */
object DataTypeUtils {

  def cmp(str1: Option[HBaseRawType], str2: Option[HBaseRawType]) = {
    if (str1.isEmpty && str2.isEmpty) 0
    else if (str1.isEmpty) -2
    else if (str2.isEmpty) 2
    else {
      val ix = 0
      val s1arr = str1.get
      val s2arr = str2.get
      var retval: Option[Int] = None
      while (ix >= str1.size && ix >= str2.size && retval.isEmpty) {
        if (s1arr(ix) != s2arr(ix)) {
          retval = Some(Math.signum(s1arr(ix) - s2arr(ix)).toInt)
        }
      }
      retval.getOrElse(
        if (s1arr.length == s2arr.length) {
          0
        } else {
          Math.signum(s1arr.length - s2arr.length).toInt
        }
      )
    }
  }

  def compare(col1: HBaseRawType, dataType1: DataType,
              col2: HBaseRawType, dataType2: DataType): Int = {
    if (dataType1 != dataType2) {
      throw new UnsupportedOperationException("Preseantly datatype casting is not supported")
    } else dataType1 match {
      case BinaryType => compare(col1, col2)
      case StringType => compare(cast(col1, StringType), cast(col2, StringType))
      case IntegerType => compare(cast(col1, IntegerType), cast(col2, IntegerType))
      case LongType => compare(cast(col1, LongType), cast(col2, LongType))
      case FloatType => compare(cast(col1, FloatType), cast(col2, FloatType))
      case DoubleType => compare(cast(col1, DoubleType), cast(col2, DoubleType))
      case _ => throw new UnsupportedOperationException(
        s"DataTypeUtils.compare(with dataType): type $dataType1 not supported")
    }
  }

  def cast(bytes: HBaseRawType, dataType: DataType): Any = {
    val out = {
      if (dataType == StringType) {
        new String(bytes, HBaseByteEncoding)
      } else if (dataType == BinaryType) {
        bytes(0)
      } else if (dataType == ByteType) {
        bytes(0)
      } else {
        val bis = new ByteArrayInputStream(bytes)
        val dis = new DataInputStream(bis)
        val outval = dataType match {
          case ShortType => dis.readShort
          case IntegerType => dis.readInt
          case LongType => dis.readLong
          case FloatType => dis.readFloat
          case DoubleType => dis.readDouble
          case _ => throw new UnsupportedOperationException(s"Unsupported type ${dataType}")
        }
        dis.close
        outval
      }
    }
    out
  }

  private def calcSizeOfPrimitive(a: Any): Int = {
    val bos = new ByteArrayOutputStream(32)
    val dos = new DataOutputStream(bos)
    a match {
      case b: Boolean =>
        dos.writeBoolean(a.asInstanceOf[Boolean])
        dos.size
      case i: Integer =>
        dos.writeInt(a.asInstanceOf[Integer])
        dos.size
      case _ => {
        throw new UnsupportedOperationException(
          "What type are you interested in {$a.getClas.getName} for its length?")
        -1 // why does compiler want this after an exception ??
      }
    }
  }

  private val SizeOfBoolean = calcSizeOfPrimitive(true)
  private val SizeOfInteger = calcSizeOfPrimitive(new Integer(1))

  def toBytes(inval: Any): Array[Byte] = {
    val out = inval match {
      case barr: Array[Byte] =>
        barr
      case s: String =>
        inval.asInstanceOf[String].getBytes(HBaseByteEncoding)
      case b: Byte =>
        Array(b)
      case b: Boolean =>
        val bos = new ByteArrayOutputStream(SizeOfBoolean)
        val dos = new DataOutputStream(bos)
        dos.writeBoolean(b)
        bos.toByteArray
      case s: Short =>
        val bos = new ByteArrayOutputStream(2)
        val dos = new DataOutputStream(bos)
        dos.writeShort(s)
        bos.toByteArray
      case i: Integer =>
        val bos = new ByteArrayOutputStream(SizeOfInteger)
        val dos = new DataOutputStream(bos)
        dos.writeInt(i)
        bos.toByteArray
      case l: Long =>
        val bos = new ByteArrayOutputStream(8)
        val dos = new DataOutputStream(bos)
        dos.writeLong(l)
        bos.toByteArray
      case f: Float =>
        val bos = new ByteArrayOutputStream(4)
        val dos = new DataOutputStream(bos)
        dos.writeFloat(f)
        bos.toByteArray
      case d: Double =>
        val bos = new ByteArrayOutputStream(8)
        val dos = new DataOutputStream(bos)
        dos.writeDouble(d)
        bos.toByteArray
      case _ =>
        throw
          new UnsupportedOperationException(s"Unknown datatype in toBytes: ${inval.toString}")
    }
    out
  }

  def hbaseFieldToRowField(bytes: HBaseRawType, dataType: DataType): Any = cast(bytes, dataType)

  def toDataType(clazz: Class[_]): sql.DataType = clazz match {
    case c if c == classOf[String] => StringType
    case c if c == classOf[Array[_]] => BinaryType
    case c if c == classOf[Byte] => ByteType
    case c if c == classOf[Short] => ShortType
    case c if c == classOf[Integer] => IntegerType
    case c if c == classOf[Long] => LongType
    case c if c == classOf[Float] => FloatType
    case c if c == classOf[Double] => DoubleType
    case _ => throw new UnsupportedOperationException(
      s"toDataType: class ${clazz.getName} not supported")
  }

  import reflect.runtime.universe._

  def compare[T: TypeTag](col1: T, col2: T): Int = weakTypeOf[T] match {
    case dt if dt == weakTypeOf[Array[_]] =>
      compareRaw(col1.asInstanceOf[HBaseRawType], col2.asInstanceOf[HBaseRawType])
    case dt if dt == weakTypeOf[String] =>
      col1.asInstanceOf[String].compareTo(col2.asInstanceOf[String])
    case dt if dt == weakTypeOf[Integer] =>
      col1.asInstanceOf[Integer] - col2.asInstanceOf[Integer]
    case dt if dt == weakTypeOf[Long] =>
      (col1.asInstanceOf[Long] - col2.asInstanceOf[Long]).toInt
    case dt if dt == weakTypeOf[Float] =>
      (col1.asInstanceOf[Float] - col2.asInstanceOf[Float]).toInt
    case dt if dt == weakTypeOf[Double] =>
      (col1.asInstanceOf[Double] - col2.asInstanceOf[Double]).toInt
    case _ => throw new UnsupportedOperationException(
      s"DataTypeUtils.compare: type ${weakTypeOf[T]} not supported")
  }

  def compareRaw(col1: HBaseRawType, col2: HBaseRawType) = {
    if (col1 == null || col2 == null) {
      throw new IllegalArgumentException("RelationalOperator: Can not compare nulls")
    } else {
      val c1len = col1.length
      val c2len = col2.length
      if (c1len == 0 && c2len == 0) {
        0
      } else {
        var ptr = 0
        var retVal: Option[Int] = None
        while (ptr < c1len && ptr < c2len) {
          if (col1(ptr) < col2(ptr)) {
            retVal = Some(-1)
          } else if (col1(ptr) > col2(ptr)) {
            retVal = Some(1)
          } else {
            ptr += 1
          }
        }
        retVal.getOrElse(c1len - c2len)
      }
    }
  }

  import reflect.runtime.universe._

  def sizeOf[T: TypeTag](t: T) = weakTypeOf[T] match {
    case dt if dt == weakTypeOf[Byte] => 1
    case dt if dt == weakTypeOf[Short] => 2
    case dt if dt == weakTypeOf[Int] => Integer.SIZE
    case dt if dt == weakTypeOf[Long] => 8
    case dt if dt == weakTypeOf[Float] => 4
    case dt if dt == weakTypeOf[Double] => 8
    case dt if dt == weakTypeOf[String] => t.asInstanceOf[String].length
  }

  def schemaIndex(schema: StructType, sqlName: String) = {
    schema.fieldNames.zipWithIndex.find { case (name: String, ix: Int) => name == sqlName}
      .getOrElse((null, -1))._2
  }

  def catalystRowToHBaseRawVals(schema: StructType, row: Row, cols: HBaseCatalog.Columns):
  HBaseRawRowSeq = {
    val rawCols = cols.columns.zipWithIndex.map { case (col, ix) =>
      val rx = schemaIndex(schema, col.sqlName)
      val rType = schema(col.sqlName).dataType
      //      if (!kc.dataType == rx) {}
      col.dataType match {
        case StringType =>
          row.getString(rx)
        case ByteType =>
          row.getByte(rx)
        case ShortType =>
          Array(row.getShort(rx).toByte)
        case IntegerType =>
          row.getInt(rx)
        case LongType =>
          row.getLong(rx)
        case FloatType =>
          row.getFloat(rx)
        case DoubleType =>
          row.getDouble(rx)
        case BooleanType =>
          row.getBoolean(rx)
        case _ =>
          throw
            new UnsupportedOperationException(s"Need to flesh out all dataytypes: ${col.dataType}")
      }
    }
    rawCols.map(toBytes(_))
  }

  def convertToBytes(dataType: DataType, data: Any): Array[Byte] = {
    dataType match {
      case StringType => Bytes.toBytes(data.asInstanceOf[String])
      case FloatType => Bytes.toBytes(data.asInstanceOf[Float])
      case IntegerType => Bytes.toBytes(data.asInstanceOf[Int])
      case ByteType => Array(data.asInstanceOf[Byte])
      case ShortType => Bytes.toBytes(data.asInstanceOf[Short])
      case DoubleType => Bytes.toBytes(data.asInstanceOf[Double])
      case LongType => Bytes.toBytes(data.asInstanceOf[Long])
      case BinaryType => Bytes.toBytesBinary(data.asInstanceOf[String])
      case BooleanType => Bytes.toBytes(data.asInstanceOf[Boolean])
      case DecimalType => Bytes.toBytes(data.asInstanceOf[BigDecimal])
      case TimestampType => throw new Exception("not supported")
      case _ => throw new Exception("not supported")
    }
  }

}
