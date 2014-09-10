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
import org.apache.hadoop.hbase.client.{HTable, HBaseAdmin, HConnectionManager, HTableInterface}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(hbaseContext: HBaseSQLContext) extends Catalog with Logging {
  lazy val configuration = hbaseContext.sparkContext.getConf.get("hadoop.configuration")
    .asInstanceOf[Configuration]
  lazy val hbaseConnection = {
    val connection = HConnectionManager.createConnection(configuration)
    connection
  }
  val logger = Logger.getLogger(getClass.getName)
  val caseSensitive: Boolean = false

  override def unregisterAllTables(): Unit = {}

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit = ???

  override def lookupRelation(databaseName: Option[String], tableName: String,
                              alias: Option[String]): LogicalPlan = {
    val itableName = processTableName(tableName)
    val table = getHBaseTable(itableName)
    val h : HTable = null

    new HBaseRelation(tableName, alias)(table,hbaseContext.getPartitions(tableName))(hbaseContext)
  }

  def getHBaseTable(tableName: String): HTableInterface = {
    hbaseConnection.getTable(tableName)
  }

  protected def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  def createTable(tableName: String, columnFamily: String): Unit = {
    val admin = new HBaseAdmin(hbaseConnection)
    val descriptor = new HTableDescriptor(TableName.valueOf(tableName))

    val columnDescriptor = new HColumnDescriptor(Bytes.toBytes(columnFamily))
    descriptor.addFamily(columnDescriptor)

    admin.createTable(descriptor)
  }

  def deleteTable(tableName: String): Unit = {
    val admin = new HBaseAdmin(hbaseConnection)

    admin.disableTable(tableName)
    admin.deleteTable(tableName)
  }

  override def registerTable(databaseName: Option[String], tableName: String,
                             plan: LogicalPlan): Unit = ???


  case class Column(cf: String, cq: String)

  class Columns(val columns: Seq[Column]) {

    import collection.mutable

    val colsMap = columns.foldLeft(mutable.Map[String, Column]()) { case (m, c) =>
      m(s"$c.cf:$c.cq") = c
      m
    }
  }

  case class HBaseTable(tableName: String, rowKey: RowKey, cols: Columns)

  sealed trait RowKey

  case object RawBytesRowKey extends RowKey

  case class TypedRowKey(columns: Columns) extends RowKey

}
