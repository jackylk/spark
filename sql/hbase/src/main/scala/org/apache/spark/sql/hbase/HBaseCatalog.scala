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
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(hbaseContext: HBaseSQLContext,
                                   configuration : Configuration)
      extends Catalog with Logging {

  import HBaseCatalog._

  lazy val hconnection = HBaseUtils.getHBaseConnection(configuration)

  val tables = new HashMap[String, LogicalPlan]()
  val logger = Logger.getLogger(getClass.getName)
  val caseSensitive: Boolean = false

  // TODO(Bo): read the entire HBASE_META_TABLE and process it once, then cache it
  // in this class
  override def unregisterAllTables(): Unit = {
    tables.clear
  }

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit =
    tables -= tableName

  /**
   * Retrieve table from catalog given the SQL name
   * @param sqlTableName
   * @return
   */
  def getTable(sqlTableName: String) = {
    val tableName: TableName = null
    val rowKey: TypedRowKey = null
    val colFamilies: Set[String] = null
    val columns: Columns = null
    HBaseCatalogTable(sqlTableName, tableName, rowKey, colFamilies, columns,
      HBaseUtils.getPartitions(tableName, configuration))
  }

  /**
   * Retrieve table from catalog given the HBase (namespace,tablename)
   */
  def getTable(tableName: TableName) = {
    val sqlTableName = null
    val rowKey: TypedRowKey = null
    val colFamilies: Set[String] = null
    val columns: Columns = null
    HBaseCatalogTable(sqlTableName, tableName, rowKey, colFamilies, columns,
      HBaseUtils.getPartitions(tableName, configuration))
  }

  // TODO: determine how to look it up
  def getExternalResource(tableName: TableName) = ???

  override def lookupRelation(nameSpace: Option[String], unqualTableName: String,
                              alias: Option[String]): LogicalPlan = {
    val itableName = processTableName(unqualTableName)
    val catalogTable = getTable("DEFAULT",
      TableName.valueOf(nameSpace.orNull, unqualTableName).getNameAsString)
    val tableName = TableName.valueOf(nameSpace.orNull, itableName)
    val externalResource = getExternalResource(tableName)
    new HBaseRelation(/* configuration, hbaseContext, htable, */ catalogTable, externalResource)
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

  def getTable(dbName: String, tableName: String): HBaseCatalogTable = {

    val table = new HTable(configuration, MetaData)

    val get = new Get(Bytes.toBytes(dbName + "." + tableName))
    val rest1 = table.get(get)

    var columnInfo = Bytes.toString(rest1.getValue(ColumnFamily, QualColumnInfo))
    if (columnInfo.length > 0) {
      columnInfo = columnInfo.substring(0, columnInfo.length - 1)
    }
    val columnInfoArray = columnInfo.split(",")
    var infoColumns = List[Column]()
    for (column <- columnInfoArray) {
      val index = column.indexOf("=")
      val sqlName = column.substring(0, index)
      val value = column.substring(index + 1).toUpperCase()
      val dataType = HBaseDataType.withName(value)

      // TODO(Bo): add the catalyst column name and the family to the Column object
      val col = Column(sqlName, null, null, dataType)
      infoColumns = infoColumns :+ col
    }

    val hbaseName = Bytes.toString(rest1.getValue(ColumnFamily, QualHbaseName))

    var mappingInfo = Bytes.toString(rest1.getValue(ColumnFamily, QualMappingInfo))
    if (mappingInfo.length > 0) {
      mappingInfo = mappingInfo.substring(0, mappingInfo.length - 1)
    }
    val mappingInfoArray = mappingInfo.split(",")
    var mappingColumns = List[Column]()
    var colFamilies = Set[String]()
    for (mapping <- mappingInfoArray) {
      val index = mapping.indexOf("=")
      val sqlName = mapping.substring(0, index)
      val value = mapping.substring(index + 1)
      val split = value.indexOf(".")
      val family = value.substring(0, split)
      val qualifier = value.substring(split + 1)

      colFamilies = colFamilies + family
      val col = Column(sqlName, family, qualifier, null)
      mappingColumns = mappingColumns :+ col
    }

    var columnList = List[Column]()
    for (column <- infoColumns) {
      val result = mappingColumns.find(e => e.sqlName.equals(column.sqlName))
      if (result.isEmpty) {
        val col = Column(column.sqlName, column.family, column.qualifier, column.dataType)
        columnList = columnList :+ col
      }
      else {
        val head = result.head
        val col = Column(head.sqlName, head.family, head.qualifier, column.dataType)
        columnList = columnList :+ col
      }
    }
    val columns = new Columns(columnList)

    var keys = Bytes.toString(rest1.getValue(ColumnFamily, QualKeys))
    if (keys.length > 0) {
      keys = keys.substring(0, keys.length - 1)
    }
    val keysArray = keys.split(",")
    var keysList = List[Column]()
    for (key <- keysArray) {
      val col = Column(key, null, null, null)
      keysList = keysList :+ col
    }
    val rowKey = TypedRowKey(new Columns(keysList))

    val tName = TableName.valueOf(tableName)
    HBaseCatalogTable(hbaseName, tName, rowKey,
      colFamilies,
      columns,
      HBaseUtils.getPartitions(tName, configuration))
  }

  def createMetadataTable(admin: HBaseAdmin) = {
    val desc = new HTableDescriptor(TableName.valueOf(MetaData))
    val coldef = new HColumnDescriptor(ColumnFamily)
    desc.addFamily(coldef)
    admin.createTable(desc)
  }

  def createTable(dbName: String, tableName: String,
                  hbaseTableName: String,
                  keyColumns: Seq[KeyColumn],
                  nonKeyColumns: Columns
                  ): Unit = {
    //println(System.getProperty("java.class.path"))

    val admin = new HBaseAdmin(configuration)

    val avail = admin.isTableAvailable(MetaData)

    if (!avail) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    table.setAutoFlushTo(false)
    val rowKey = dbName + "." + tableName

    val get = new Get(Bytes.toBytes(rowKey))
    if (table.exists(get)) {
      throw new Exception("row key exists")
    }
    else {
      val put = new Put(Bytes.toBytes(rowKey))

      val result1 = new StringBuilder
      for (column <- nonKeyColumns.columns) {
        val key = column.qualifier
        val value = column.dataType
        result1.append(key)
        result1.append("=")
        result1.append(value.toString)
        result1.append(",")
      }
      put.add(ColumnFamily, QualColumnInfo, Bytes.toBytes(result1.toString))

      val result2 = new StringBuilder
      result2.append(hbaseTableName)
      put.add(ColumnFamily, QualHbaseName, Bytes.toBytes(result2.toString))

      val result3 = new StringBuilder
      // TODO(Bo): fix
//      for ((key, value) <- mappingInfo) {
//        result3.append(key)
//        result3.append("=")
//        result3.append(value)
//        result3.append(",")
//      }
      put.add(ColumnFamily, QualMappingInfo, Bytes.toBytes(result3.toString))

      val result4 = new StringBuilder
      for (key <- keyColumns) {
        result4.append(key.sqlName)
        result4.append(",")
      }
      put.add(ColumnFamily, QualKeys, Bytes.toBytes(result4.toString))

      table.put(put)

      table.flushCommits()
    }
  }

  override def registerTable(databaseName: Option[String], tableName: String,
                             plan: LogicalPlan): Unit = ???

}

object HBaseCatalog {

  val MetaData = "metadata"
  val ColumnFamily = Bytes.toBytes("colfam")
  val QualKeys = Bytes.toBytes("keys")
  val QualColumnInfo = Bytes.toBytes("columnInfo")
  val QualHbaseName = Bytes.toBytes("hbaseName")
  val QualMappingInfo = Bytes.toBytes("mappingInfo")

  object HBaseDataType extends Enumeration {
    val STRING, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN = Value
  }

  sealed trait RowKey

  case class Column(sqlName: String, family: String, qualifier: String,
                    dataType: HBaseDataType.Value,
                    ordinal: Int = Column.nextOrdinal) {
    def fullName = s"$family:$qualifier"

    def toColumnName = ColumnName(family, qualifier)
  }

  case class KeyColumn(sqlName: String, dataType: HBaseDataType.Value)

  object Column {
    private val colx = new java.util.concurrent.atomic.AtomicInteger

    def nextOrdinal = colx.getAndIncrement

    def toAttribute(col: Column): Attribute = null

    //      AttributeReference(
    //      col.family,
    //      col.dataType,
    //      nullable=true
    //    )()
  }

  class Columns(val columns: Seq[Column]) {

    import scala.collection.mutable

    val colx = new java.util.concurrent.atomic.AtomicInteger

    def apply(colName: ColumnName) = {
      map(colName)
    }

    def apply(colName: String): Option[Column] = {
      val Pat = "(.*):(.*)".r
      colName match {
        case Pat(colfam, colqual) => lift(map(ColumnName(colfam, colqual)))
        case sqlName: String => findBySqlName(sqlName)
      }
    }

    def findBySqlName(sqlName: String): Option[Column] = {
      map.iterator.find { case (cname, col) =>
        col.sqlName == sqlName
      }.map(_._2)
    }

    import scala.collection.mutable

    private val map: mutable.Map[ColumnName, Column] =
      columns.foldLeft(mutable.Map[ColumnName, Column]()) { case (m, c) =>
        m(ColumnName(c.family, c.qualifier)) = c
        m
      }

    def getColumn(colName: String): Option[Column] = map.get(ColumnName(colName))

    def families() = Set(columns.map(_.family))

    def asAttributes() = {
      columns.map { col =>
        Column.toAttribute(col)
      }
    }

    def lift[A: reflect.ClassTag](a: A): Option[A] = a match {
      case a: Some[A] => a
      case None => None
      case a: A => Some(a)
    }
  }

  case class HBaseCatalogTable(catalystTablename: String,
                               tableName: TableName,
                               rowKey: TypedRowKey,
                               colFamilies: Set[String],
                               columns: Columns,
                               partitions: Seq[HBasePartition])

  case class TypedRowKey(columns: Columns) extends RowKey

  case object RawBytesRowKey extends RowKey

}
