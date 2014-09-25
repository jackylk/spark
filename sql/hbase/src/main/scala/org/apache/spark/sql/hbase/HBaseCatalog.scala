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
private[hbase] class HBaseCatalog(hbaseContext: HBaseSQLContext) extends Catalog with Logging {
  lazy val configuration = HBaseUtils.configuration
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
  def getTableFromCatalog(sqlTableName : String) = {
    val tableName : TableName = null
    val rowKey : TypedRowKey = null
    val colFamilies : Set[String] = null
    val columns : Columns = null
    HBaseCatalogTable(sqlTableName, tableName, rowKey, colFamilies, columns,
      HBaseUtils.getPartitions(tableName))
  }

  /**
   * Retrieve table from catalog given the HBase (namespace,tablename)
   */
  def getTableFromCatalog(tableName : TableName) = {
    val sqlTableName = null
    val rowKey : TypedRowKey = null
    val colFamilies : Set[String] = null
    val columns : Columns = null
    HBaseCatalogTable(sqlTableName, tableName, rowKey, colFamilies, columns,
      HBaseUtils.getPartitions(tableName))
  }

  // TODO: determine how to look it up
  def getExternalResource(tableName : TableName) = ???

  override def lookupRelation(nameSpace: Option[String], unqualTableName: String,
                              alias: Option[String]): LogicalPlan = {
    val itableName = processTableName(unqualTableName)
    val catalogTable = getTableFromCatalog("DEFAULT",
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

  def getTableFromCatalog(dbName: String, tableName: String): HBaseCatalogTable = {
    val conf = HBaseConfiguration.create()

    val table = new HTable(conf, METADATA)

    val get = new Get(Bytes.toBytes(dbName + "." + tableName))
    val rest1 = table.get(get)

    var columnInfo = Bytes.toString(rest1.getValue(COLUMN_FAMILY, QUAL_COLUMN_INFO))
    if (columnInfo.length > 0) {
      columnInfo = columnInfo.substring(0, columnInfo.length - 1)
    }
    val columnInfoArray = columnInfo.split(",")
    var columns = List[Column]()
    for (column <- columnInfoArray) {
      val index = column.indexOf("=")
      val key = column.substring(0, index)
      val value = column.substring(index + 1).toUpperCase()
      val t = HBaseDataType.withName(value)

      // TODO(Bo): add the catalyst column name and the family to the Column object
      val col = Column(null, null, key, t)
      columns = columns :+ col
    }
    val columnInfoList = new Columns(columns)

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
    var keysList = List[Column]()
    for (key <- keysArray) {
      val col = Column(null, null, key, null)
      keysList = keysList :+ col
    }
    val keysInfoList = TypedRowKey(new Columns(keysList))

    // TODO(Bo): fix up for new structure
    // HBaseCatalogTable( dbName, tableName, columnInfoList, hbaseName,
    // keysInfoList, mappingInfoList)
    null
  }

  def createTable(dbName: String, tableName: String, columnInfo: Columns,
                  hbaseTableName: String, keys: List[String],
                  mappingInfo: List[(String, String)]): Unit = {
    //println(System.getProperty("java.class.path"))

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
      for (column <- columnInfo.columns) {
        // TODO(bo): handle the catalystColname and hbase family name
        val key = column.qualifier
        val value = column.dataType
        result1.append(key)
        result1.append("=")
        result1.append(value.toString)
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

  def retrieveTable(dbName: String, tableName: String): HBaseCatalogTable = {
//  def retrieveTable(dbName: String, tableName: String): (List[(String, String)],
//    String, List[String], List[(String, String)]) = {
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

//    (columnInfoList, hbaseName, keysList.toList, mappingInfoList)
    null // TODO(Bo): Make return value of HBaseCatalogTable
        // BTW should we just go ahead and return an HBaseRelation??
  }

  override def registerTable(databaseName: Option[String], tableName: String,
                             plan: LogicalPlan): Unit = ???

  sealed trait RowKey

  case class Column(sqlName : String, family: String, qualifier: String,
                    dataType : HBaseDataType.Value,
                    ordinal : Int = Column.nextOrdinal) {
    def fullName = s"$family:$qualifier"
    def toColumnName = ColumnName(family, qualifier)
  }

  object Column {
    private val colx = new java.util.concurrent.atomic.AtomicInteger
    def nextOrdinal = colx.getAndIncrement

    def toAttribute(col : Column) : Attribute = null
//      AttributeReference(
//      col.family,
//      col.dataType,
//      nullable=true
//    )()
  }
  class Columns(val columns: Seq[Column]) {
    val colx = new java.util.concurrent.atomic.AtomicInteger

    def apply(colName : ColumnName) = {
      map(colName)
    }

    def lift[A : reflect.ClassTag](a : A) : Option[A] = a match {
      case a : Some[A] => a
      case None => None
      case a : A =>  Some(a)
    }
    def apply(colName : String) : Option[Column] = {
      val Pat = "(.*):(.*)".r
      colName match {
        case Pat(colfam, colqual) => lift(map(ColumnName(colfam, colqual)))
        case sqlName : String => findBySqlName(sqlName)
      }
    }

    def findBySqlName(sqlName : String) : Option[Column] = {
      map.iterator.find{ case (cname, col) =>
        col.sqlName == sqlName
      }.map(_._2)
    }
    import scala.collection.mutable

    private val map : mutable.Map[ColumnName, Column] =
      columns.foldLeft(mutable.Map[ColumnName, Column]()) { case (m, c) =>
      m(ColumnName(c.family,c.qualifier)) = c
      m
    }

    def getColumn(colName : String) : Option[Column] = map.get(ColumnName(colName))

    def families() = Set(columns.map(_.family))

    def asAttributes() = {
      columns.map { col =>
        Column.toAttribute(col)
      }
    }
  }

  case class HBaseCatalogTable(catalystTablename : String,
                               tableName: TableName,
                               rowKey: TypedRowKey,
                               colFamilies : Set[String],
                               columns: Columns,
                               partitions : Seq[HBasePartition] )

  case class TypedRowKey(columns: Columns) extends RowKey

  case object RawBytesRowKey extends RowKey

}

object HBaseDataType extends Enumeration {
  val STRING, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN = Value
}
