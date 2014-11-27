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

import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.catalyst.types._

/**
 * Data Type conversion utilities
 *
 */
object DataTypeUtils {
  //  TODO: more data types support?
  def bytesToData (src: HBaseRawType,
                  dt: DataType): Any = {
    dt match {
      case StringType => BytesUtils.toString(src)
      case IntegerType => BytesUtils.toInt(src)
      case BooleanType => BytesUtils.toBoolean(src)
      case ByteType => src(0)
      case DoubleType => BytesUtils.toDouble(src)
      case FloatType => BytesUtils.toFloat(src)
      case LongType => BytesUtils.toLong(src)
      case ShortType => BytesUtils.toShort(src)
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def setRowColumnFromHBaseRawType(row: MutableRow,
                                   index: Int,
                                   src: HBaseRawType,
                                   dt: DataType): Unit = {
    dt match {
      case StringType => row.setString(index, BytesUtils.toString(src))
      case IntegerType => row.setInt(index, BytesUtils.toInt(src))
      case BooleanType => row.setBoolean(index, BytesUtils.toBoolean(src))
      case ByteType => row.setByte(index, BytesUtils.toByte(src))
      case DoubleType => row.setDouble(index, BytesUtils.toDouble(src))
      case FloatType => row.setFloat(index, BytesUtils.toFloat(src))
      case LongType => row.setLong(index, BytesUtils.toLong(src))
      case ShortType => row.setShort(index, BytesUtils.toShort(src))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }

  def getRowColumnFromHBaseRawType(row: Row,
                                   index: Int,
                                   dt: DataType): HBaseRawType = {
    dt match {
      case StringType => BytesUtils.toBytes(row.getString(index))
      case IntegerType => BytesUtils.toBytes(row.getInt(index))
      case BooleanType => BytesUtils.toBytes(row.getBoolean(index))
      case ByteType => BytesUtils.toBytes(row.getByte(index))
      case DoubleType => BytesUtils.toBytes(row.getDouble(index))
      case FloatType => BytesUtils.toBytes(row.getFloat(index))
      case LongType => BytesUtils.toBytes(row.getLong(index))
      case ShortType => BytesUtils.toBytes(row.getShort(index))
      case _ => throw new Exception("Unsupported HBase SQL Data Type")
    }
  }
}
