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
import org.apache.spark.sql.parquet.ParquetTableScan
import org.apache.spark.sql.{SQLContext, SchemaRDD, StructType}

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
        if (HBaseStrategies.PushDownPredicates) {
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
        }

        val emptyPredicate = ColumnPredicate.EmptyColumnPredicate

        val rowKeyColumnPredicates = Some(Seq(ColumnPredicate.EmptyColumnPredicate))

        // TODO(sboesch):  create multiple HBaseSQLTableScan's based on the calculated partitions
        def partitionRowKeyPredicatesByHBasePartition(rowKeyPredicates:
                                                      Option[Seq[ColumnPredicate]]):
        Seq[Seq[ColumnPredicate]] = {
          //TODO(sboesch): map the row key predicates to the
          // respective physical HBase Region server ranges
          //  and return those as a Sequence of ranges
          // First cut, just return a single range - thus we end up with a single HBaseSQLTableScan
          Seq(rowKeyPredicates.getOrElse(Seq(ColumnPredicate.EmptyColumnPredicate)))
        }

        val partitionRowKeyPredicates =
          partitionRowKeyPredicatesByHBasePartition(rowKeyColumnPredicates)

        //        partitionRowKeyPredicates.flatMap { partitionSpecificRowKeyPredicates =>
        def projectionToHBaseColumn(expr: NamedExpression,
                                    hbaseRelation: HBaseRelation): ColumnName = {
          hbaseRelation.catalogTable.allColumns.findBySqlName(expr.name).map(_.toColumnName).get
        }

        val columnNames = projectList.map(projectionToHBaseColumn(_, relation))

        val effectivePartitionSpecificRowKeyPredicates =
          if (rowKeyColumnPredicates == ColumnPredicate.EmptyColumnPredicate) {
            None
          } else {
            rowKeyColumnPredicates
          }

        val scanBuilder: (Seq[Attribute] => SparkPlan) = HBaseSQLTableScan(
          _,
          partitionKeyIds.toSeq,
          relation,
          columnNames,
          predicates.reduceLeftOption(And),
          rowKeyPredicates.reduceLeftOption(And),
          effectivePartitionSpecificRowKeyPredicates,
          externalResource,
          plan)(hbaseContext)

        pruneFilterProject(
          projectList,
          Nil, // otherPredicates,
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

      val rowKeysWithRows = childRdd.zip(rowKeysFromRows(childRdd, relation))

      putToHBase(schema, relation, hbContext, rowKeysWithRows)
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
      assertFromClosure(childRdd != null, "InsertIntoHBaseTable: the child RDD is empty")

      val rowKeysWithRows = childRdd.zip(rowKeysFromRows(childRdd, relation))

      putToHBase(schema, relation, hbContext, rowKeysWithRows)
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

  val PushDownPredicates = false

  // WIP
  def putToHBase(rddSchema: StructType,
                 relation: HBaseRelation,
                 @transient hbContext: HBaseSQLContext,
                 rowKeysWithRows: RDD[(Row, HBaseRawType)]) = {

    val contextInfo = (hbContext.catalog,
      hbContext.serializeProps) // TODO: we need the externalresource as well
    rowKeysWithRows.mapPartitions { partition =>
      if (!partition.isEmpty) {
        println("we are running the putToHBase..")
        var hbaseConf = HBaseConfiguration.create // SparkHadoopUtil.get.newConfiguration
        readFieldsIntoConfFromSerializedProps(hbaseConf, contextInfo._2)
        val hConnection = HConnectionManager.createConnection(hbaseConf)
        val tableIf = hConnection.getTable(relation.catalogTable.hbaseTableName.tableName)
        partition.map { case (row, rkey) =>
          val put = relation.rowToHBasePut(rddSchema, row)
          tableIf.put(put)
          if (!partition.hasNext) {
            hConnection.close
            tableIf.close
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

  // For Testing ..
  def putToHBaseLocal(rddSchema: StructType,
                      relation: HBaseRelation,
                      @transient hbContext: HBaseSQLContext,
                      rowKeysWithRows: RDD[(Row, HBaseRawType)]) = {

    val contextInfo = (hbContext.catalog, hbContext.serializeProps) // TODO: add externalresource
    val localData = rowKeysWithRows.collect
    println(s"RowCount is ${rowKeysWithRows.count}")
    var hbaseConf = HBaseConfiguration.create // SparkHadoopUtil.get.newConfiguration
    val hConnection = HConnectionManager.createConnection(hbaseConf)
    val tableIf = hConnection.getTable(relation.catalogTable.hbaseTableName.tableName)
    localData.zipWithIndex.map { case ((row, rkey), ix) =>
      println("we are running the putToHBase..")
      val put = relation.rowToHBasePut(rddSchema, row)
      tableIf.put(put)

      val get = tableIf.get(new Get(rkey))
      val map = get.getNoVersionMap
      val fname = s"/tmp/row$ix"
      writeToFile(fname, s"rowkey=${new String(get.getRow)} map=${map.toString}")

    }
    tableIf.close
    println("Hey we finished the putToHBase..")
    localData

    def writeToFile(fname: String, msg: Any) = {
      msg match {
        case s: String =>
          val pw = new PrintWriter(new FileWriter(fname))
          pw.write(s)
          pw.close
        case arr: Array[Byte] =>
          val os = new FileOutputStream(fname)
          os.write(arr)
          os.close
        case x =>
          val pw = new PrintWriter(new FileWriter(fname))
          pw.write(x.toString)
          pw.close
      }
    }
  }

  def rowKeysFromRows(schemaRdd: SchemaRDD, relation: HBaseRelation) = {
    assert(schemaRdd != null)
    assert(relation != null)
    assert(relation.rowKeyParser != null)
    schemaRdd.map { r: Row =>
      relation.rowKeyParser.createKeyFromCatalystRow(
        schemaRdd.schema,
        relation.catalogTable.rowKeyColumns,
        r)
    }
  }

  def readFieldsIntoConfFromSerializedProps(conf: Configuration, serializedProps: Array[Byte]) = {
    val conf = HBaseConfiguration.create
    val bis = new ByteArrayInputStream(serializedProps)
    conf.readFields(new DataInputStream(bis))
    conf
  }

}
