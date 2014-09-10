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
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.spark.{Partitioner, RangePartitioner, SparkContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{catalyst, SQLConf, SQLContext, SchemaRDD}
import org.apache.hadoop.hbase._

import scala.collection.JavaConverters


/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 */
class HBaseSQLContext(sc: SparkContext, hbaseConf : Configuration
            = HBaseConfiguration.create())
            extends SQLContext(sc) {
  self =>

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
      BroadcastNestedLoopJoin
    )
  }

  @transient
  override protected[sql] val planner = hbasePlanner

  @transient
  private[hbase] val hconnection = HConnectionManager.createConnection(hbaseConf)

  // Change the default SQL dialect to HiveQL
  override private[spark] def dialect: String = getConf(SQLConf.DIALECT, "hbaseql")

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  /** Extends QueryExecution with HBase specific features. */
  protected[sql] abstract class QueryExecution extends super.QueryExecution {
  }

  @transient
  override protected[sql] def parser = new HBaseSQLParser

  override def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hbaseql") {
      new SchemaRDD(this, HBaseQl.parseSql(sqlText))
    }  else {
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

  def getPartitions(tableName : String) = {
    import JavaConverters._
    val regionLocations = hconnection.locateRegions(TableName.valueOf(tableName))
    case class Bounds(startKey : String, endKey : String)
    val regionBounds = regionLocations.asScala.map{ hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      Bounds( new String(regionInfo.getStartKey), new String(regionInfo.getEndKey))
    }
    regionBounds.zipWithIndex.map{ case (rb,ix) =>
      new HBasePartition(ix, (rb.startKey, rb.endKey))
    }
  }

  def close() = {
    hconnection.close
  }
}
