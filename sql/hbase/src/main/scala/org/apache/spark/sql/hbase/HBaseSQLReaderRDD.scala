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

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.{Partitioner, Partition, TaskContext}

/**
 * HBaseSQLReaderRDD
 * Created by sboesch on 9/16/14.
 */
class HBaseSQLReaderRDD(tableName: TableName,
                        externalResource: HBaseExternalResource,
                        hbaseRelation: HBaseRelation,
                        projList: Seq[ColumnName],
                        //      rowKeyPredicates : Option[Seq[ColumnPredicate]],
                        //      colPredicates : Option[Seq[ColumnPredicate]],
                        partitions: Seq[HBasePartition],
                        colFamilies: Set[String],
                        colFilters: Option[FilterList],
                          @transient hbaseContext: HBaseSQLContext,
                        @transient plan: LogicalPlan)
  extends HBaseSQLRDD(tableName, externalResource, hbaseContext, plan) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val conn = Some(externalResource.getConnection(HBaseUtils.configuration(),
      hbaseRelation.tableName))
    try {
      val hbPartition = split.asInstanceOf[HBasePartition]
      val scan = new Scan(hbPartition.bounds.start.asInstanceOf[Array[Byte]],
        hbPartition.bounds.end.asInstanceOf[Array[Byte]])
      colFamilies.foreach { cf =>
        scan.addFamily(s2b(cf))
      }
      colFilters.map { flist => scan.setFilter(flist)}
      scan.setMaxVersions(1)
      val htable = conn.get.getTable(hbaseRelation.tableName)
      val scanner = htable.getScanner(scan)
      new Iterator[Row] {

        import collection.mutable

        val map = new mutable.HashMap[String, HBaseRawType]()

        def toRow(result: Result, projList: Seq[ColumnName]) :  HBaseRow = {
          // TODO(sboesch): analyze if can be multiple Cells in the result
          // Also, consider if we should go lower level to the cellScanner()
          val vmap = result.getNoVersionMap
          val rowArr = projList.zipWithIndex.
            foldLeft(new Array[HBaseRawType](projList.size)) { case (arr, (cname, ix)) =>
            arr(ix) = vmap.get(s2b(projList(ix).fullName)).asInstanceOf[HBaseRawType]
            arr
          }
          new HBaseRow(rowArr)
        }

        var onextVal: Option[HBaseRow] = None

        def nextRow() : Option[HBaseRow] = {
          val result = scanner.next
          if (result!=null) {
            onextVal = Some(toRow(result, projList))
            onextVal
          } else {
            None
          }
        }

        override def hasNext: Boolean = {
          if (onextVal.isDefined) {
            true
          } else {
            nextRow.isDefined
          }
        }
        override def next(): Row = {
          nextRow()
          onextVal.get
        }
      }
    } finally {
      // TODO: set up connection caching possibly by HConnectionPool
      if (!conn.isEmpty) {
        externalResource.releaseConnection(conn.get)
      }
    }
  }

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition) : Seq[String]
    = super.getPreferredLocations(split)


}
