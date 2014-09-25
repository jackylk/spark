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
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._

//import org.apache.spark.sql.execution.SparkStrategies.HashAggregation


/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(sc: SparkContext, hbaseConf: Configuration
= HBaseConfiguration.create())
  extends SQLContext(sc) {
  self =>
  @transient val configuration = hbaseConf

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
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin,
      HbaseStrategy(self)
    )

    case class HbaseStrategy(context: HBaseSQLContext) extends Strategy{

      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case CreateTablePlan(tableName, tableCols, hbaseTable, keys, otherCols) => {
          Seq(CreateTableCommand(tableName, tableCols, hbaseTable, keys, otherCols)(context))
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

  override def parseSql(sql: String): LogicalPlan = parser(sql)

  override def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hbaseql") {
      new SchemaRDD(this, parser(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hbaseql'")
    }
  }

  override lazy val analyzer = new Analyzer(catalog,
    functionRegistry, true) {
  }

  def createHbaseTable(tableName: String,
    tableCols: Seq[(String, String)],
    hbaseTable: String,
    keys: Seq[String],
    otherCols: Seq[(String, String)]): Unit = {
    val columnInfo = new catalog.Columns(tableCols.map{
      // TODO(Bo): reconcile the invocation of Column including catalystName and hbase family
      case(name, dataType) => catalog.Column(null, null, name, HBaseDataType.withName(dataType))
    })
    // TODO(Bo): reconcile the invocation of createTable to the Catalog
    catalog.createTable("DEFAULT", tableName, null /*tableCols.toList */, hbaseTable, keys.toList,
      otherCols.toList)
  }

  def close() = {
    hconnection.close
  }
}

case class CreateTableCommand(tableName: String,
                              tableCols: Seq[(String, String)],
                              hbaseTable: String,
                              keys: Seq[String],
                              otherCols: Seq[(String, String)])(@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.createHbaseTable(tableName, tableCols, hbaseTable, keys, otherCols)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}
