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

import org.apache.hadoop.hbase.util.Bytes

class BytesUtils {
  lazy val booleanArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_BOOLEAN)
  lazy val byteArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_BYTE)
  lazy val charArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_CHAR)
  lazy val doubleArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_DOUBLE)
  lazy val floatArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_FLOAT)
  lazy val intArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_INT)
  lazy val longArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_LONG)
  lazy val shortArray: Array[Byte] = new Array[Byte](Bytes.SIZEOF_SHORT)

  def toBytes(input: String): Array[Byte] = {
    Bytes.toBytes(input)
  }

  def toString(input: HBaseRawType): String = {
    Bytes.toString(input)
  }

  def toBytes(input: Byte): Array[Byte] = {
    //    byteArray(0) = input
    //    byteArray
    // Flip sign bit so that Byte is binary comparable
    byteArray(0) = (input ^ 0x80).asInstanceOf[Byte]
    byteArray
  }

  def toByte(input: HBaseRawType): Byte = {
    //    input(0)
    // Flip sign bit back
    val v: Int = input(0) ^ 0x80
    v.asInstanceOf[Byte]
  }

  def toBytes(input: Boolean): Array[Byte] = {
    if (input) {
      booleanArray(0) = (-1).asInstanceOf[Byte]
    }
    else {
      booleanArray(0) = 0.asInstanceOf[Byte]
    }
    booleanArray
  }

  def toBoolean(input: HBaseRawType): Boolean = {
    Bytes.toBoolean(input)
  }

  def toBytes(input: Double): Array[Byte] = {
    val bits: Long = java.lang.Double.doubleToRawLongBits(input)
    toBytes(bits)
  }

  def toDouble(input: HBaseRawType): Double = {
    Bytes.toDouble(input)
  }

  def toBytes(input: Short): Array[Byte] = {
    //    shortArray(1) = input.asInstanceOf[Byte]
    //    shortArray(0) = (input >> 8).asInstanceOf[Byte]
    //    shortArray
    shortArray(0) = ((input >> 8) ^ 0x80).asInstanceOf[Byte]
    shortArray(1) = input.asInstanceOf[Byte]
    shortArray
  }

  def toShort(input: HBaseRawType): Short = {
    //    Bytes.toShort(input)
    // flip sign bit back
    var v: Int = input(0) ^ 0x80
    v = (v << 8) + (input(1) & 0xff)
    v.asInstanceOf[Short]
  }

  def toBytes(input: Float): Array[Byte] = {
    //    val bits: Int = java.lang.Float.floatToRawIntBits(input)
    //    toBytes(bits)
    var i: Int = java.lang.Float.floatToIntBits(input)
    i = (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1
    toBytes(i)
  }

  def toFloat(input: HBaseRawType): Float = {
    //    Bytes.toFloat(input)
    var i = toInt(input)
    i = i - 1
    i ^= (~i >> Integer.SIZE - 1) | Integer.MIN_VALUE
    java.lang.Float.intBitsToFloat(i)
  }

  def toBytes(input: Int): Array[Byte] = {
    //    var value: Int = input
    //    for (i <- 3 to 1 by -1) {
    //      intArray(i) = value.asInstanceOf[Byte]
    //      value = value >>> 8
    //    }
    //    intArray(0) = value.asInstanceOf[Byte]
    //    intArray

    // Flip sign bit so that INTEGER is binary comparable
    intArray(0) = ((input >> 24) ^ 0x80).asInstanceOf[Byte]
    intArray(1) = (input >> 16).asInstanceOf[Byte]
    intArray(2) = (input >> 8).asInstanceOf[Byte]
    intArray(3) = input.asInstanceOf[Byte]
    intArray
  }

  def toInt(input: HBaseRawType): Int = {
    //    Bytes.toInt(input)

    // Flip sign bit back
    var v: Int = input(0) ^ 0x80
    for (i <- 1 to Bytes.SIZEOF_INT - 1) {
      v = (v << 8) + (input(i) & 0xff)
    }

    v
  }

  def toBytes(input: Long): Array[Byte] = {
    var value: Long = input
    for (i <- 7 to 1 by -1) {
      longArray(i) = value.asInstanceOf[Byte]
      value = value >>> 8
    }
    longArray(0) = value.asInstanceOf[Byte]
    longArray
  }

  def toLong(input: HBaseRawType): Long = {
    Bytes.toLong(input)
  }
}
