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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, _}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.parquet.{ParquetTableScan, ParquetFilters, ParquetRelation}


/**
 * HBaseStrategies
 * Created by sboesch on 8/22/14.
 */
private[hbase] trait HBaseStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  val hbaseContext: HBaseSQLContext

  /**
   * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HBaseTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: HBaseRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.

        val partitionKeys = relation.catalogTable.rowKey.columns.asAttributes

        val partitionKeyIds = AttributeSet(partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition {
          _.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          predicates, // As opposed to hive, hbase requires all predicates for the Scan's
          identity[Seq[Expression]],
        null) :: Nil
//          HBaseTableScan(partitionKeyIds, relation, predicates,
//              pruningPredicates.reduceLeftOption(And))(hbaseContext)) :: Nil
        Nil
      case _ =>
        Nil
    }
  }

  case class RandomAccessByRowkey(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] =  {
      // val b = new Batch
      throw new UnsupportedOperationException("RandomAccessByRowkey not yet implemented")
    }
  }

  case class SequentialScan(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] =  {
      val scan = new Scan

      throw new UnsupportedOperationException("RandomAccessByRowkey not yet implemented")
    }
  }

  def getHTable(conf : Configuration, tname : String) = {
    val htable = new HTable(conf, tname)
    htable
  }

  def sparkFilterProjectJoinToHBaseScan(sFilter : Filter,
                                          sProject : Projection, sJoin : Join) = {
//    if (sFilter.child.

  }
  def sequentialScan(htable : HTable, filter : HFilter) = {
//    val htable
  }

  object HBaseOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
//      case PhysicalOperation(projectList, filters: Seq[Expression], relation: HBaseRelation) =>
//          val hBaseColumns = projectList.map{ p =>
//
//          new HBaseSQLReaderRDD()
      case _ => Nil
    }
  }

}
