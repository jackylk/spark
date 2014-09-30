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

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, _}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hbase.HBaseCatalog.Columns
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
      case PhysicalOperation(projectList,
      inPredicates,
      relation: HBaseRelation) =>

        val predicates = inPredicates.asInstanceOf[Seq[BinaryExpression]]
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.

        val partitionKeys = relation.catalogTable.rowKey.columns.asAttributes()

        val partitionKeyIds = AttributeSet(partitionKeys)
        var (rowKeyPredicates, _ /*otherPredicates*/ ) = predicates.partition {
          _.references.subsetOf(partitionKeyIds)
        }

        val externalResource = relation.getExternalResource

        // Find and sort all of the rowKey dimension elements and stop as soon as one of the
        // composite elements is not found in any predicate
        val loopx = new AtomicLong
        val foundx = new AtomicLong
        val rowPrefixPredicates = for {pki <- partitionKeyIds
                                       if ((loopx.incrementAndGet >= 0)
                                         && rowKeyPredicates.flatMap {
                                         _.references
                                       }.contains(pki)
                                         && (foundx.incrementAndGet == loopx.get))
                                       attrib <- rowKeyPredicates.filter {
                                         _.references.contains(pki)
                                       }
        } yield attrib

        val otherPredicates = predicates.filterNot(rowPrefixPredicates.toList.contains)

        def rowKeyOrdinal(name: ColumnName) = relation.catalogTable.rowKey.columns(name).ordinal

        val catColumns: Columns = relation.catalogTable.columns
        val keyColumns: Columns = relation.catalogTable.rowKey.columns
        def catalystToHBaseColumnName(catColName: String) = {
          catColumns.findBySqlName(catColName)
        }

        // TODO(sboesch): uncertain if nodeName were canonical way to get correct sql column name
        def getName(expression: NamedExpression) = expression.asInstanceOf[NamedExpression].name

        val sortedRowPrefixPredicates = rowPrefixPredicates.toList.sortWith { (a, b) =>
          keyColumns(getName(a.left.asInstanceOf[NamedExpression])).
            get.ordinal <= keyColumns(getName(b.left.asInstanceOf[NamedExpression])).get.ordinal
        }

        // TODO(sboesch): complete the (start_key,end_key) calculations

        // We are only pushing down predicates in which one side is a column and the other is
        // a literal. Column to column comparisons are not initially supported. Therefore
        // check for each predicate containing only ONE reference
        //        val allPruningPredicateReferences = pruningPredicates.filter(pp =>
        //          pp.references.size == 1).flatMap(_.references)

        // Pushdown for RowKey filtering is only supported for prefixed rows so we
        // stop as soon as one component of the RowKey has no predicate
        //   val pruningPrefixIds = for {pki <- partitionKeyIds; pprid <-
        //     allPruningPredicateReferences.filter { pr : Attribute  => pr.exprId == pki.exprId}}
        //     yield pprid


        // If any predicates passed all restrictions then let us now build the RowKeyFilter
        var invalidRKPreds = false
        var rowKeyColumnPredicates: Option[Seq[ColumnPredicate]] =
          if (!sortedRowPrefixPredicates.isEmpty) {
            val bins = rowKeyPredicates.map {
              case pp: BinaryComparison =>
                Some(ColumnPredicate.catalystToHBase(pp))
              case s =>
                log.info(s"RowKeyPreds: Only BinaryComparison operators supported ${s.toString}")
                invalidRKPreds = true
                None
            }.flatten
            if (!bins.isEmpty) {
              Some(bins)
            } else {
              None
            }
          } else {
            None
          }
        if (invalidRKPreds) {
          rowKeyColumnPredicates = None
        }
        // TODO(sboesch): map the RowKey predicates to the Partitions
        // to achieve Partition Pruning.

        // Now process the projection predicates
        var invalidPreds = false
        var colPredicates = if (!predicates.isEmpty) {
          predicates.map {
            case pp: BinaryComparison =>
              Some(ColumnPredicate.catalystToHBase(pp))
            case s =>
              log.info(s"ColPreds: Only BinaryComparison operators supported ${s.toString}")
              invalidPreds = true
              None
          }
        } else {
          None
        }
        if (invalidPreds) {
          colPredicates = None
        }

        val emptyPredicate = ColumnPredicate.EmptyColumnPredicate
        // TODO(sboesch):  create multiple HBaseSQLTableScan's based on the calculated partitions
        def partitionRowKeyPredicatesByHBasePartition(rowKeyPredicates:
                                                      Option[Seq[ColumnPredicate]]): Seq[Seq[ColumnPredicate]] = {
          //TODO(sboesch): map the row key predicates to the
          // respective physical HBase Region server ranges
          //  and return those as a Sequence of ranges
          // First cut, just return a single range - thus we end up with a single HBaseSQLTableScan
          Seq(rowKeyPredicates.getOrElse(Seq(ColumnPredicate.EmptyColumnPredicate)))
        }

        val partitionRowKeyPredicates =
          partitionRowKeyPredicatesByHBasePartition(rowKeyColumnPredicates)

        partitionRowKeyPredicates.flatMap { partitionSpecificRowKeyPredicates =>
          def projectionToHBaseColumn(expr: NamedExpression,
                                      hbaseRelation: HBaseRelation): ColumnName = {
            hbaseRelation.catalogTable.columns.findBySqlName(expr.name).map(_.toColumnName).get
          }

          val columnNames = projectList.map(projectionToHBaseColumn(_, relation))

          val effectivePartitionSpecificRowKeyPredicates =
            if (rowKeyColumnPredicates == ColumnPredicate.EmptyColumnPredicate) {
              None
            } else {
              rowKeyColumnPredicates
            }

          val scanBuilder = HBaseSQLTableScan(partitionKeyIds.toSeq,
            relation,
            columnNames,
            predicates.reduceLeftOption(And),
            rowKeyPredicates.reduceLeftOption(And),
            effectivePartitionSpecificRowKeyPredicates,
            externalResource,
            plan)(hbaseContext).asInstanceOf[Seq[Expression] => SparkPlan]

          pruneFilterProject(
            projectList,
            otherPredicates,
            identity[Seq[Expression]], // removeRowKeyPredicates,
            scanBuilder) :: Nil
        }
      case _ =>
        Nil
    }
  }

  case class RandomAccessByRowkey(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      // val b = new Batch
      throw new UnsupportedOperationException("RandomAccessByRowkey not yet implemented")
    }
  }

  case class SequentialScan(context: SQLContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      val scan = new Scan

      throw new UnsupportedOperationException("RandomAccessByRowkey not yet implemented")
    }
  }

  def getHTable(conf: Configuration, tname: String) = {
    val htable = new HTable(conf, tname)
    htable
  }

  def sparkFilterProjectJoinToHBaseScan(sFilter: Filter,
                                        sProject: Projection, sJoin: Join) = {
    //    if (sFilter.child.

  }

  def sequentialScan(htable: HTable, filter: HFilter) = {
    //    val htable
  }

  object HBaseOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTablePlan(tableName, hbaseTableName, keyCols, nonKeyCols) =>
        Seq(CreateTableCommand(tableName, hbaseTableName, keyCols, nonKeyCols)(hbaseContext))
      case _ => Nil
    }
  }
}
