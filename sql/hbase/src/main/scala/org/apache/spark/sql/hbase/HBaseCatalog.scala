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
import org.apache.hadoop.hbase.client.{HTableInterface, HConnectionManager}
import org.apache.log4j.Logger

/* Implicit conversions */
import scala.collection.JavaConversions._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{EliminateAnalysisOperators, Catalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.util.Utils

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(hbasectx: HBaseSQLContext) extends Catalog with Logging  {
  val logger = Logger.getLogger(getClass.getName)

  lazy val conf = hbasectx.sparkContext.getConf.get("hadoop.configuration").asInstanceOf[Configuration]
  lazy val hbaseConn = {
    val conn = HConnectionManager.createConnection(conf)
    conn
  }
  def getHBaseTable(tname : String) = {
    hbaseConn.getTable(tname)
  }

  def lookupRelation(
    tableName: String,
  alias: Option[String]) : LogicalPlan = synchronized {
    val tblName = processTableName(tableName)
    val table =
}
