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
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._

//import org.apache.spark.sql.execution.SparkStrategies.HashAggregation

import scala.collection.JavaConverters


/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(sc: SparkContext, hbaseConf: Configuration
= HBaseConfiguration.create())
  extends SQLContext(sc) {
  self =>

  @transient
  override protected[sql] lazy val catalog: HBaseCatalog = new HBaseCatalog(this)

  @transient val hbasePlanner = new SparkPlanner with HBaseStrategies {
    val hbaseContext = self

    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      TakeOrdered,
      ParquetOperations,
      InMemoryScans,
      HBaseTableScans,
      //      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin,
      HbaseStrategy(self)
    )

    case class HbaseStrategy(context: HBaseSQLContext) extends Strategy{

      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case CreateTablePlan(a, b, c, d, e) => {
          println("In HbaseStrategy")
          Seq(CreateTableCommand(a,b,c,d,e)(context))
        };
        case _ => Nil
      }
    }
  }

  @transient
  override protected[sql] val planner = hbasePlanner

  @transient
  private[hbase] val hconnection = HConnectionManager.createConnection(hbaseConf)

  override private[spark] val dialect: String = "hbaseql"

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution {
      val logical = plan
    }

  /** Extends QueryExecution with HBase specific features. */
  protected[sql] abstract class QueryExecution extends super.QueryExecution {
  }

  @transient
  override protected[sql] val parser = new HBaseSQLParser

  override def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hbaseql") {
      new SchemaRDD(this, parser(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hbaseql'")
    }
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   */
  def analyze(tableName: String) {
    throw new UnsupportedOperationException("analyze not yet supported for HBase")
  }

  def getPartitions(tableName: String) = {
    import scala.collection.JavaConverters._
    val regionLocations = hconnection.locateRegions(TableName.valueOf(tableName))
    case class Bounds(startKey: String, endKey: String)
    val regionBounds = regionLocations.asScala.map { hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      Bounds(new String(regionInfo.getStartKey), new String(regionInfo.getEndKey))
    }
    regionBounds.zipWithIndex.map { case (rb, ix) =>
      new HBasePartition(ix, (rb.startKey, rb.endKey))
    }
  }

  def createHbaseTable(tableName: String,
                       tableCols: Seq[(String, String)],
                       hbaseTable: String,
                       keys: Seq[String],
                       otherCols: Seq[Expression]): Unit = {
    println("in createHbaseTable")
    val colsTypeMap: Map[String, String] = 
      tableCols.map{case(colName, colType) => colName -> colType}.toMap
    val otherColsMap:Map[String, String] =
      otherCols.map{case EqualTo(e1, e2) => e1.toString.substring(1) -> e2.toString.substring(1)}.toMap
    catalog.createTable("DEFAULT", tableName, colsTypeMap, hbaseTable, keys.toList, otherColsMap);
  }

  def close() = {
    hconnection.close
  }
}

case class CreateTableCommand(tableName: String,
                              tableCols: Seq[(String, String)],
                              hbaseTable: String,
                              keys: Seq[String],
                              otherCols: Seq[Expression])(@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.createHbaseTable(tableName, tableCols, hbaseTable, keys, otherCols)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}