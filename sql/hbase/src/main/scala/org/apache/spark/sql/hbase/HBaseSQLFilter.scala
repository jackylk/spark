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

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter._
import org.apache.log4j.Logger
import HBaseUtils._

/**
 * HBaseSQLFilter: a set of PushDown filters for optimizing Column Pruning
 * and Row Filtering by using HBase Scan/Filter constructs
 *
 * Created by sboesch on 9/22/14.
 */
class HBaseSQLFilters(colFamilies: Set[String], rowKeyPreds: Option[Seq[ColumnPredicate]],
                      opreds: Option[Seq[ColumnPredicate]], rowKeyParser: RowKeyParser)
  extends FilterBase {
  val logger = Logger.getLogger(getClass.getName)

  def createColumnFilters(): Option[FilterList] = {
    val colFilters: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    colFilters.addFilter(new HBaseRowFilter(colFamilies, rowKeyParser, rowKeyPreds.orNull))
    val filters = opreds.map {
      case preds: Seq[ColumnPredicate] =>
        preds.filter { p: ColumnPredicate =>
          // TODO(sboesch): the second condition is not compiling
          (p.right.isInstanceOf[HLiteral] || p.left.isInstanceOf[HLiteral])
          /* && (p.right.isInstanceOf[HColumn] || p.left.isInstanceOf[HColumn]) */
        }.map { p =>
          var col: HColumn = null
          var colval: HLiteral = null

          if (p.right.isInstanceOf[HLiteral]) {
            col = p.left.asInstanceOf[HColumn]
            colval = p.right.asInstanceOf[HLiteral]
          } else {
            col = p.right.asInstanceOf[HColumn]
            colval = p.left.asInstanceOf[HLiteral]
          }
          new SingleColumnValueFilter(s2b(col.colName.family),
            s2b(col.colName.qualifier),
            p.op.toHBase,
            new BinaryComparator(s2b(colval.litval.toString)))
        }.foreach { f =>
          colFilters.addFilter(f)
        }
        colFilters
    }
    filters
  }
}

/**
 * Presently only a sequence of AND predicates supported. TODO(sboesch): support simple tree
 * of AND/OR predicates
 */
class HBaseRowFilter(colFamilies: Set[String], rowKeyParser: RowKeyParser,
                     rowKeyPreds: Seq[ColumnPredicate]
 /*, preds: Seq[ColumnPredicate] */) extends FilterBase {
  val logger = Logger.getLogger(getClass.getName)

  override def filterRowKey(rowKey: Array[Byte], offset: Int, length: Int): Boolean = {
    val rowKeyColsMap = rowKeyParser.parseRowKeyWithMetaData(rowKey.slice(offset, offset + length))
    val result = rowKeyPreds.forall { p =>
      var col: HColumn = null
      var colval: HLiteral = null

      val passFilter = p.right match {
        case a : HLiteral => {
          col = p.left.asInstanceOf[HColumn]
          colval = p.right.asInstanceOf[HLiteral]
          // TODO(sboesch): handle proper conversion of datatypes to bytes
          p.op.cmp(rowKeyColsMap(col.colName), colval.litval.toString.getBytes)
        }
        case _ => {
          col = p.right.asInstanceOf[HColumn]
          colval = p.left.asInstanceOf[HLiteral]
          // TODO(sboesch): handle proper conversion of datatypes to bytes
          p.op.cmp(colval.litval.toString.getBytes, rowKeyColsMap(col.colName))
        }
      }
      passFilter
    }
    result
  }

  override def filterKeyValue(ignored: Cell): ReturnCode = {
    null
  }

  override def isFamilyEssential(name: Array[Byte]): Boolean = {
    colFamilies.contains(new String(name, ByteEncoding).toLowerCase())
  }

  override def filterRowCells(ignored: util.List[Cell]): Unit = super.filterRowCells(ignored)

}
