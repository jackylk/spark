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
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable

/**
 * HBaseSQLReaderRDD
 * Created by sboesch on 9/16/14.
 */
class HBaseSQLReaderRDD(tableName: SerializableTableName,
                        externalResource: Option[HBaseExternalResource],
                        hbaseRelation: HBaseRelation,
                        projList: Seq[ColumnName],
                        //      rowKeyPredicates : Option[Seq[ColumnPredicate]],
                        //      colPredicates : Option[Seq[ColumnPredicate]],
                        partitions: Seq[HBasePartition],
                        colFamilies: Seq[String],
                        colFilters: Option[FilterList],
                        @transient hbaseContext: HBaseSQLContext)
  extends HBaseSQLRDD(tableName, externalResource, partitions, hbaseContext) {

  val applyFilters = false

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    val hbPartition = split.asInstanceOf[HBasePartition]
    val scan = if (applyFilters) {
      new Scan(hbPartition.bounds.start.get,
        hbPartition.bounds.end.get)
    } else {
      new Scan
    }
    if (applyFilters) {
      colFamilies.foreach { cf =>
        scan.addFamily(s2b(cf))
      }

      colFilters.map { flist => scan.setFilter(flist)}
    }
    // scan.setMaxVersions(1)

    @transient val htable = new HTable(configuration, tableName.tableName)
    @transient val scanner = htable.getScanner(scan)
    //      @transient val scanner = htable.getScanner(scan)
    new Iterator[Row] {

      import scala.collection.mutable

      val map = new mutable.HashMap[String, HBaseRawType]()

      var onextVal: Row = _

      def nextRow(): Row = {
        val result = scanner.next
        if (result != null) {
          onextVal = toRow(result, projList)
          onextVal
        } else {
          null
        }
      }

      val ix = new java.util.concurrent.atomic.AtomicInteger()

      override def hasNext: Boolean = {
        ix.incrementAndGet <= 2
      }

      override def next(): Row = {
        nextRow()
        onextVal
      }
    }
  }

  def toRow(result: Result, projList: Seq[ColumnName]): Row = {
    // TODO(sboesch): analyze if can be multiple Cells in the result
    // Also, consider if we should go lower level to the cellScanner()
    val row = result.getRow
    val rkCols = hbaseRelation.catalogTable.rowKeyColumns
    val rowKeyMap = RowKeyParser.parseRowKeyWithMetaData(rkCols.columns, row)
    var rmap = new mutable.HashMap[String, Any]()

    rkCols.columns.foreach { rkcol =>
      rmap.update(rkcol.toString, rowKeyMap(rkcol.toColumnName))
    }

    val jmap = new java.util.TreeMap[Array[Byte], Array[Byte]](Bytes.BYTES_COMPARATOR)
    rmap.foreach { case (k, v) =>
      jmap.put(s2b(k), CatalystToHBase.toBytes(v))
    }
    import collection.JavaConverters._
    val vmap = result.getNoVersionMap
    vmap.put(s2b(""), jmap)
    val rowArr = projList.zipWithIndex.
      foldLeft(new Array[HBaseRawType](projList.size)) {
      case (arr, (cname, ix)) =>
        arr(ix) = vmap.get(s2b(projList(ix).family.getOrElse("")))
          .get(s2b(projList(ix).qualifier))
        arr
    }
    Row(rowArr)
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  override private[spark] def computeOrReadCheckpoint(split: Partition,
                                                      context: TaskContext): Iterator[Row]
  = super.computeOrReadCheckpoint(split, context)


}
