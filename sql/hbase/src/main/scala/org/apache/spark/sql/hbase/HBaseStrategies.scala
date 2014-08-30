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
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericMutableRow, Expression, Projection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.parquet.{ParquetTableScan, ParquetFilters, InsertIntoParquetTable, ParquetRelation}
import org.apache.spark.sql.{execution, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Join, Filter, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan

/**
 * HBaseStrategies
 * Created by sboesch on 8/22/14.
 */
object HBaseStrategies extends SQLContext#SparkPlanner {

  self: SQLContext#SparkPlanner =>

  object HBaseOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters: Seq[Expression], relation: HBaseRelation) =>
        val prunePushedDownFilters =
          if (sparkContext.conf.getBoolean(HBaseFilters.HBASE_FILTER_PUSHDOWN_ENABLED, true)) {
            (filters: Seq[Expression]) => {
              filters.filter { filter =>
                val recordFilter = HBaseFilters.createFilter(filter)
                if (!recordFilter.isDefined) {
                  // First case: the pushdown did not result in any record filter.
                  true
                } else {
                  // Second case: a record filter was created; here we are conservative in
                  // the sense that even if "A" was pushed and we check for "A AND B" we
                  // still want to keep "A AND B" in the higher-level filter, not just "B".
                  !ParquetFilters.findExpression(recordFilter.get, filter).isDefined
                }
              }
            }
          } else {
            identity[Seq[Expression]] _
          }
        pruneFilterProject(
          projectList,
          filters,
          prunePushedDownFilters,
          ParquetTableScan(_, relation, filters)) :: Nil

      case _ => Nil
    }
  }

  //  private[hbase] val
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

//  def sparkFilterProjectJoinToHBaseScan(sFilter : Filter,
//                                          sProject : Projection, sJoin : Join) = {
//    if (sFilter.child.
//
//  }
////  def sequentialScan(htable : HTable, filter : HFilter) = {
//  def sequentialScan(htable : HTable, filter : HFilter) = {
//    val htable
//  }


  private[sql] object HBaseRelation {
    def enableLogForwading() {
      val hbaseLogger = java.util.logging.Logger.getLogger("hbase")
      hbaseLogger.getHandlers.foreach(hbaseLogger.removeHandler)
      if (!hbaseLogger.getUseParentHandlers) {
        hbaseLogger.setUseParentHandlers(true)
      }
    }
    type RowType = GenericMutableRow
//    type CompressionType =

    def create(pathString: String,
      child: LogicalPlan,
    conf: Configuration,
    sqlContext: SQLContext) : HBaseRelation = {
      if (!child.resolved) {
        throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create HBase table from unresolved child (when schemia is not available")
      }
      createEmpty(pathString, child.output, false, conf, sqlContext)
    }

    def createEmpty(pathString: String,
    atributes: Seq[Attribute],
    allowExisting: Boolean,
    conf: Configuration,
    sqlContext: SQLContext): HBaseRelation = {
      val path = checkPath(pathString, allowExisting, conf


  }

}
