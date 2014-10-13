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

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hbase.HBaseCatalog.{KeyColumn, Column, Columns}

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(@transient val sc: SparkContext, @transient val hbaseConf: Configuration
= HBaseConfiguration.create())
  extends SQLContext(sc) with Serializable {
  self =>

  @transient val configuration = hbaseConf

  def serializeProps = {
    val bos = new ByteArrayOutputStream
    val props = hbaseConf.write(new DataOutputStream(bos))
    bos.toByteArray
  }

  @transient
  override protected[sql] lazy val catalog: HBaseCatalog = new HBaseCatalog(this, configuration)

  @transient val hBasePlanner = new SparkPlanner with HBaseStrategies {

    //    self: SQLContext#SparkPlanner =>

    val hbaseContext = self
    SparkPlan.currentContext.set(self)

    override val strategies: Seq[Strategy] = Seq(
      CommandStrategy(self),
      TakeOrdered,
      //      ParquetOperations,
      InMemoryScans,
      HBaseTableScans,
      HashAggregation,
      LeftSemiJoin,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin,
      HBaseOperations
    )
  }

  @transient
  override protected[sql] val planner = hBasePlanner

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

  def createHbaseTable(nameSpace: String,
                       tableName: String,
                       hbaseTable: String,
                       keyCols: Seq[(String, String)],
                       nonKeyCols: Seq[(String, String, String, String)]): Unit = {
    val keyColumns = keyCols.map { case (name, typeOfData) =>
      KeyColumn(name, catalog.getDataType(typeOfData.toLowerCase))
    }
    val nonKeyColumns = new Columns(nonKeyCols.map {
      case (name, typeOfData, family, qualifier) =>
        Column(name, family, qualifier, catalog.getDataType(typeOfData))
    })

    catalog.createTable(nameSpace, tableName, hbaseTable, keyColumns, nonKeyColumns)
  }

  def dropHbaseTable(tableName: String): Unit = {
    catalog.deleteTable(tableName)
  }

  def stop() = {
    hconnection.close
    sparkContext.stop()
  }
}

object HBaseSQLContext {
}
