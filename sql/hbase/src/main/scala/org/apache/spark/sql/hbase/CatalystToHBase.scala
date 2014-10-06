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

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructType}
import org.apache.spark.sql.hbase.HBaseCatalog.HBaseDataType._

/**
 * CatalystToHBase
 * Created by sboesch on 10/1/14.
 */
object CatalystToHBase {
  @transient val logger = Logger.getLogger(getClass.getName)

  def schemaIndex(schema: StructType, sqlName: String) = {
    schema.fieldNames.zipWithIndex.find { case (name: String, ix: Int) => name == sqlName}
      .getOrElse((null, -1))._2
  }
  def toBytes(inval: Any): Array[Byte] = {
    inval match {
      // TODO: use proper serialization for all datatypes instead of this to/from string hack
      case barr: Array[Byte] =>
        barr
      case s: String =>
        s.getBytes(HBaseByteEncoding)
      case b: Byte =>
        Array(b)
      case b: Boolean =>
        b.toString.getBytes(HBaseByteEncoding)
      case i: Integer =>
        i.toString.getBytes(HBaseByteEncoding)
      case l: Long =>
        l.toString.getBytes(HBaseByteEncoding)
      case f: Float =>
        f.toString.getBytes(HBaseByteEncoding)
      case d: Double =>
        d.toString.getBytes(HBaseByteEncoding)
      case _ =>
        throw
          new UnsupportedOperationException(s"Unknown datatype in toBytes: ${inval.toString}")
    }
  }
  def catalystRowToHBaseRawVals(schema : StructType, row: Row, cols: HBaseCatalog.Columns):
      HBaseRawRowSeq = {
    val rawCols = cols.columns.zipWithIndex.map { case (col, ix) =>
      val rx = schemaIndex(schema, col.sqlName)
      val rType = schema(col.sqlName).dataType
      //      if (!kc.dataType == rx) {}
      col.dataType match {
        case STRING =>
          if (rType != StringType) {
          }
          row.getString(rx)
        case BYTE =>
          row.getByte(rx)
        case SHORT =>
          Array(row.getShort(rx).toByte)
        case INTEGER =>
          row.getInt(rx)
        case LONG =>
          row.getLong(rx)
        case FLOAT =>
          row.getFloat(rx)
        case DOUBLE =>
          row.getDouble(rx)
        case BOOLEAN =>
          row.getBoolean(rx)
        case _ =>
          throw
            new UnsupportedOperationException(s"Need to flesh out all dataytypes: ${col.dataType}")
      }
    }
    rawCols.map(toBytes(_))
  }

}
