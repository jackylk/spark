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
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HConnectionManager, HTable, HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.DataType

import scala.collection.mutable.{HashMap, LinkedHashMap, ListBuffer}

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(hbaseContext: HBaseSQLContext) extends Catalog with Logging {
  lazy val configuration = HBaseUtils.getConfiguration(hbaseContext)
  lazy val hconnection = HBaseUtils.getHBaseConnection(configuration)

  val METADATA = "metadata"
  val COLUMN_FAMILY = Bytes.toBytes("colfam")
  val QUAL_KEYS = Bytes.toBytes("keys")
  val QUAL_COLUMN_INFO = Bytes.toBytes("columnInfo")
  val QUAL_HBASE_NAME = Bytes.toBytes("hbaseName")
  val QUAL_MAPPING_INFO = Bytes.toBytes("mappingInfo")
  val tables = new HashMap[String, LogicalPlan]()
  val logger = Logger.getLogger(getClass.getName)
  val caseSensitive: Boolean = false

  // TODO(Bo): read the entire HBASE_META_TABLE and process it once, then cache it
  // in this class
  override def unregisterAllTables(): Unit = { tables.clear }

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit =
    tables -= tableName

  def getTableFromCatalog(tableName : TableName) = {
    val rowKey : TypedRowKey = null
    val columns : Columns = null
    HBaseCatalogTable(tableName, rowKey, columns)
  }
  override def lookupRelation(nameSpace: Option[String], tableName: String,
                              alias: Option[String]): LogicalPlan = {
    val itableName = processTableName(tableName)
    val htable = getHBaseTable(TableName.valueOf(nameSpace.orNull, itableName))
    val catalogTable = getTableFromCatalog(TableName.valueOf(nameSpace.orNull, tableName))
    new HBaseRelation(configuration, hbaseContext, htable, catalogTable)
  }

  def getHBaseTable(tableName: TableName): HTableInterface = {
    hconnection.getTable(tableName)
  }

  protected def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

<<<<<<< HEAD
  def createTable(dbName: String, tableName: String, columnInfo: List[(String, String)],
                  hbaseTableName: String, keys: List[String],
                  mappingInfo: List[(String, String)]): Unit = {
    val conf = HBaseConfiguration.create

    val admin = new HBaseAdmin(conf)

    val avail = admin.isTableAvailable(METADATA)

    if (!avail) {
      // create table
      val desc = new HTableDescriptor(TableName.valueOf(METADATA))
      val coldef = new HColumnDescriptor(COLUMN_FAMILY)
      desc.addFamily(coldef)
      admin.createTable(desc)
    }

    val table = new HTable(conf, METADATA)
    table.setAutoFlushTo(false)
    val rowKey = dbName + "." + tableName

    val get = new Get(Bytes.toBytes(rowKey))
    if (table.exists(get)) {
      throw new Exception("row key exists")
    }
    else {
      val put = new Put(Bytes.toBytes(rowKey))

      val result1 = new StringBuilder
      for ((key, value) <- columnInfo) {
        result1.append(key)
        result1.append("=")
        result1.append(value)
        result1.append(",")
      }
      put.add(COLUMN_FAMILY, QUAL_COLUMN_INFO, Bytes.toBytes(result1.toString))

      val result2 = new StringBuilder
      result2.append(hbaseTableName)
      put.add(COLUMN_FAMILY, QUAL_HBASE_NAME, Bytes.toBytes(result2.toString))

      val result3 = new StringBuilder
      for ((key, value) <- mappingInfo) {
        result3.append(key)
        result3.append("=")
        result3.append(value)
        result3.append(",")
      }
      put.add(COLUMN_FAMILY, QUAL_MAPPING_INFO, Bytes.toBytes(result3.toString))

      val result4 = new StringBuilder
      for (key <- keys) {
        result4.append(key)
        result4.append(",")
      }
      put.add(COLUMN_FAMILY, QUAL_KEYS, Bytes.toBytes(result4.toString))

      table.put(put)

      table.flushCommits()
    }
  }

<<<<<<< HEAD
  def retrieveTable(dbName: String, tableName: String): (List[(String, String)],
    String, List[String], List[(String, String)]) = {
    val conf = HBaseConfiguration.create()

    val table = new HTable(conf, METADATA)

    val get = new Get(Bytes.toBytes(dbName + "." + tableName))
    val rest1 = table.get(get)

    var columnInfo = Bytes.toString(rest1.getValue(COLUMN_FAMILY, QUAL_COLUMN_INFO))
    if (columnInfo.length > 0) {
      columnInfo = columnInfo.substring(0, columnInfo.length - 1)
    }
    val columnInfoArray = columnInfo.split(",")
    var columnInfoList = List[(String, String)]()
    for (column <- columnInfoArray) {
      val index = column.indexOf("=")
      val key = column.substring(0, index)
      val value = column.substring(index + 1)
      columnInfoList = columnInfoList :+(key, value)
    }

    val hbaseName = Bytes.toString(rest1.getValue(COLUMN_FAMILY, QUAL_HBASE_NAME))

    var mappingInfo = Bytes.toString(rest1.getValue(COLUMN_FAMILY, QUAL_MAPPING_INFO))
    if (mappingInfo.length > 0) {
      mappingInfo = mappingInfo.substring(0, mappingInfo.length - 1)
    }
    val mappingInfoArray = mappingInfo.split(",")
    var mappingInfoList = List[(String, String)]()
    for (mapping <- mappingInfoArray) {
      val index = mapping.indexOf("=")
      val key = mapping.substring(0, index)
      val value = mapping.substring(index + 1)
      mappingInfoList = mappingInfoList :+(key, value)
    }

    var keys = Bytes.toString(rest1.getValue(COLUMN_FAMILY, QUAL_KEYS))
    if (keys.length > 0) {
      keys = keys.substring(0, keys.length - 1)
    }
    val keysArray = keys.split(",")
    var keysList = new ListBuffer[String]()
    for (key <- keysArray) {
      keysList += key
    }

    (columnInfoList, hbaseName, keysList.toList, mappingInfoList)
  }

  override def registerTable(databaseName: Option[String], tableName: String,
                             plan: LogicalPlan): Unit = ???


  case class Column(family: String, qualifier: String, dataType : DataType)

  object Column {
<<<<<<< HEAD
    def toAttribute(col : Column) : Attribute = null
//      AttributeReference(
//      col.family,
//      col.dataType,
//      nullable=true
//    )()
  }
  class Columns(val columns: Seq[Column]) {

    import scala.collection.mutable

    val colsMap = columns.foldLeft(mutable.Map[String, Column]()) { case (m, c) =>
      m(s"$c.cf:$c.cq") = c
      m
    }
    def asAttributes() = {
      columns.map{ col =>
        Column.toAttribute(col)
      }
    }
  }


  case class HBaseCatalogTable(tableName: TableName, rowKey: TypedRowKey, cols: Columns)

  sealed trait RowKey

  case object RawBytesRowKey extends RowKey

  case class TypedRowKey(columns: Columns) extends RowKey

}
