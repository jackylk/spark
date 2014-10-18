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

import java.io._
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HConnectionManager, HTable}
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{QueryPlanner, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase.HBaseCatalog.Columns
import org.apache.spark.sql.{SQLContext, SchemaRDD, StructType}

import scala.annotation.tailrec

/**
 * HBaseStrategies
 * Created by sboesch on 8/22/14.
 */

/**
 *
 *
private[sql] abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

 */
private[hbase] trait HBaseStrategies extends QueryPlanner[SparkPlan] {
  self: SQLContext#SparkPlanner =>

  import org.apache.spark.sql.hbase.HBaseStrategies._

  val hbaseContext: HBaseSQLContext


  /**
   * Retrieves data using a HBaseTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HBaseTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, inPredicates, relation: HBaseRelation) =>

        // Filter out all predicates that only deal with partition keys
        val partitionsKeys = AttributeSet(relation.partitionKeys)
        val (rowKeyPredicates, otherPredicates) = inPredicates.partition {
          _.references.subsetOf(partitionsKeys)
        }

        // TODO: Ensure the outputs from the relation match the expected columns of the query

        val predAttributes = AttributeSet(inPredicates.flatMap(_.references))
        val projectSet = AttributeSet(projectList.flatMap(_.references))

        val attributes = projectSet ++ predAttributes

        val rowPrefixPredicates = relation.getRowPrefixPredicates(rowKeyPredicates)

        //        partitionRowKeyPredicates.flatMap { partitionSpecificRowKeyPredicates =>
        def projectionToHBaseColumn(expr: NamedExpression,
                                    hbaseRelation: HBaseRelation): ColumnName = {
          hbaseRelation.catalogTable.allColumns.findBySqlName(expr.name).map(_.toColumnName).get
        }

        val rowKeyPreds: Seq[Expression] = if (!rowPrefixPredicates.isEmpty) {
          Seq(rowPrefixPredicates.reduceLeft(And))
        } else {
          Nil
        }

        val scanBuilder: (Seq[Attribute] => SparkPlan) = HBaseSQLTableScan(
          _,   // TODO: this first parameter is not used but can not compile without it
          attributes.map {
            _.toAttribute
          }.toSeq,
          relation,
          projectList,
          otherPredicates, // Assume otherPreds == columnPruningPredicates ?
          rowKeyPreds,
          rowKeyPreds,
          None // coprocSubPlan
        )(hbaseContext)

        pruneFilterProject(
          projectList,
          inPredicates,
          identity[Seq[Expression]], // removeRowKeyPredicates,
          scanBuilder) :: Nil

      case _ =>
        Nil
    }
  }

  def getHTable(conf: Configuration, tname: String) = {
    val htable = new HTable(conf, tname)
    htable
  }

  def sparkFilterProjectJoinToHBaseScan(sFilter: Filter,
                                        sProject: Projection, sJoin: Join) = {
    // TODO..
  }

  @inline def assertFromClosure(p: Boolean, msg: String) = {
    if (!p) {
      throw new IllegalStateException(s"AssertionError: $msg")
    }
  }

  case class InsertIntoHBaseTable(
                                   relation: HBaseRelation,
                                   child: SparkPlan,
                                   overwrite: Boolean = false)
                                 (hbContext: HBaseSQLContext)
    extends UnaryNode {
    override def execute() = {
      val childRdd = child.execute().asInstanceOf[SchemaRDD]
      assertFromClosure(childRdd != null, "InsertIntoHBaseTable: the source RDD failed")

      putToHBase(childRdd, relation, hbContext)
      childRdd
    }

    override def output = child.output
  }

  case class InsertIntoHBaseTableFromRdd(
                                          relation: HBaseRelation,
                                          childRdd: SchemaRDD,
                                          bulk: Boolean = false,
                                          overwrite: Boolean = false)
                                        (hbContext: HBaseSQLContext)
    extends UnaryNode {
    override def execute() = {
      assert(childRdd != null, "InsertIntoHBaseTable: the child RDD is empty")

      putToHBase(childRdd, relation, hbContext)
      childRdd
    }

    override def child: SparkPlan = SparkLogicalPlan(
      ExistingRdd(childRdd.queryExecution.executedPlan.output, childRdd))(hbContext)
      .alreadyPlanned

    override def output = child.output
  }

  object HBaseOperations extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateHBaseTablePlan(tableName, nameSpace, hbaseTableName, keyCols, nonKeyCols) =>
        Seq(CreateHBaseTableCommand(tableName, nameSpace, hbaseTableName, keyCols, nonKeyCols)
          (hbaseContext))
      case logical.InsertIntoTable(table: HBaseRelation, partition, child, overwrite) =>
        new InsertIntoHBaseTable(table, planLater(child), overwrite)(hbaseContext) :: Nil
      case DropTablePlan(tableName) => Seq(DropHbaseTableCommand(tableName)(hbaseContext))
      case _ => Nil
    }

  }

}

object HBaseStrategies {

  // TODO: set to true when the logic for PDP has been tested
  val PushDownPredicates = false

  // WIP
  def putToHBase(schemaRdd: SchemaRDD,
                 relation: HBaseRelation,
                 @transient hbContext: HBaseSQLContext) {

    val schema = schemaRdd.schema
    val serializedProps = HBaseSQLContext.serializeConfiguration(hbContext.configuration)
    schemaRdd.mapPartitions { partition =>
      if (!partition.isEmpty) {
        println("we are running the putToHBase..")
        val configuration = HBaseSQLContext.createConfigurationFromSerializedFields(serializedProps)
        val tableIf = relation.getHTable
        partition.map { case row =>
          val put = relation.buildPut(schema, row)
          tableIf.put(put)
          if (!partition.hasNext) {
            relation.closeHTable
          }
          row
        }
      } else {
        new Iterator[(Row, HBaseRawType)]() {
          override def hasNext: Boolean = false

          override def next(): (Row, HBaseRawType) = null
        }
      }
    }
  }

}
