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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{SparkContext, Partition, TaskContext}

import scala.collection.mutable

/**
 * HBaseSQLReaderRDD
 * Created by sboesch on 9/16/14.
 */

class HBaseSQLReaderRDD(relation: HBaseRelation,
                        projList: Seq[NamedExpression],
                        columnPruningPred: Seq[Expression],
                        rowKeyFilterPred: Seq[Expression],
                        partitionPred: Seq[Expression],
                        coprocSubPlan: Option[SparkPlan],
                        @transient hbaseContext: HBaseSQLContext)
  extends RDD[Row](hbaseContext.sparkContext, Nil) {

//class HBaseSQLReaderRDD(
//                        externalResource: Option[HBaseExternalResource],
//                        relation: relation,
//                        projList: Seq[NamedExpression],
//                        //      rowKeyPredicates : Option[Seq[ColumnPredicate]],
//                        //      colPredicates : Option[Seq[ColumnPredicate]],
//                        colPreds: Seq[Expression],
//                        partitions: Seq[HBasePartition],
//                        colFamilies: Seq[String],
//                        @transient hbaseContext: HBaseSQLContext)
//  extends HBaseSQLRDD(externalResource, partitions, hbaseContext) {


  @transient val logger = Logger.getLogger(getClass.getName)

  // The SerializedContext will contain the necessary instructions
  // for all Workers to know how to connect to HBase
  // For now just hardcode the Config/connection logic
  @transient lazy val configuration = relation.configuration
  @transient lazy val connection = relation.connection

  override def getPartitions: Array[Partition] = relation.getPartitions()

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity
    }.toSeq
  }

  val applyFilters: Boolean = false
  val serializedConfig = HBaseSQLContext.serializeConfiguration(configuration)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {

    relation.configuration = HBaseSQLContext
      .createConfigurationFromSerializedFields(serializedConfig)

    val scan = relation.getScanner(split)
    if (applyFilters) {
      val colFilters = relation.buildFilters(rowKeyFilterPred,columnPruningPred)
    }

    @transient val htable = relation.getHTable()
    @transient val scanner = htable.getScanner(scan)
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
        if (onextVal != null) {
          true
        } else {
          nextRow() != null
        }
      }

      override def next(): Row = {
        if (onextVal != null) {
          val tmp = onextVal
          onextVal = null
          tmp
        } else {
          nextRow
        }
      }
    }
  }

  def toRow(result: Result, projList: Seq[NamedExpression]): Row = {
    // TODO(sboesch): analyze if can be multiple Cells in the result
    // Also, consider if we should go lower level to the cellScanner()
    val row = result.getRow
    val rkCols = relation.catalogTable.rowKeyColumns
    val rowKeyMap = RowKeyParser.parseRowKeyWithMetaData(rkCols.columns, row)
    var rmap = new mutable.HashMap[String, Any]()

    rkCols.columns.foreach { rkcol =>
      rmap.update(rkcol.qualifier, rowKeyMap(rkcol.toColumnName))
    }

    val jmap = new java.util.TreeMap[Array[Byte], Array[Byte]](Bytes.BYTES_COMPARATOR)
//    rmap.foreach { case (k, v) =>
//      jmap.put(s2b(k), CatalystToHBase.toByteus(v))
//    }
    val vmap = result.getNoVersionMap
    vmap.put(s2b(""), jmap)
    val rowArr = projList.zipWithIndex.
      foldLeft(new Array[Any](projList.size)) {
      case (arr, (cname, ix)) =>
        if (rmap.get(cname.name)isDefined) {
          arr(ix) = rmap.get(cname.name).get.asInstanceOf[Tuple2[_,_]]._2
        } else {
          val col = relation.catalogTable.columns.findBySqlName(projList(ix).name).getOrElse{
            throw new IllegalArgumentException(s"Column ${projList(ix).name} not found")
          }
          val dataType =col.dataType
          val qual =s2b(col.qualifier)
          val fam = s2b(col.family)
          arr(ix) = DataTypeUtils.hbaseFieldToRowField(
              vmap.get(fam).get(qual)
            ,dataType)
        }
        arr
    }
    Row(rowArr: _*)
  }


}
