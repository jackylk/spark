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
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{SimpleCatalog}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import java.math.BigDecimal

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(hbaseContext: HBaseSQLContext,
                                   configuration : Configuration)
      extends SimpleCatalog(false) with Logging {

  import HBaseCatalog._

  lazy val hconnection = HBaseUtils.getHBaseConnection(configuration)

  val logger = Logger.getLogger(getClass.getName)

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

  def getTable(namespace: String, tableName: String): HBaseCatalogTable = {
    val table = new HTable(configuration, MetaData)

    val get = new Get(Bytes.toBytes(namespace + "." + tableName))
    val rest1 = table.get(get)

    var columnList = List[Column]()
    var columnFamilies = Set[(String)]()

    var nonKeyColumns = Bytes.toString(rest1.getValue(ColumnFamily, QualNonKeyColumns))
    if (nonKeyColumns.length > 0) {
      nonKeyColumns = nonKeyColumns.substring(0, nonKeyColumns.length - 1)
    }

    val nonKeyColumnArray = nonKeyColumns.split(";")
    for (nonKeyColumn <- nonKeyColumnArray) {
      val nonKeyColumnInfo = nonKeyColumn.split(",")
      val sqlName = nonKeyColumnInfo(0)
      val family = nonKeyColumnInfo(1)
      val qualifier = nonKeyColumnInfo(2)
      val dataType = HBaseDataType.withName(nonKeyColumnInfo(3))

      val column = Column(sqlName, family, qualifier, dataType)
      columnList = columnList :+ column
      columnFamilies = columnFamilies + family
    }

    val hbaseName = Bytes.toString(rest1.getValue(ColumnFamily, QualHbaseName))

    var keyColumns = Bytes.toString(rest1.getValue(ColumnFamily, QualKeyColumns))
    if (keyColumns.length > 0) {
      keyColumns = keyColumns.substring(0, keyColumns.length - 1)
    }
    val keyColumnArray = keyColumns.split(";")
    var keysList = List[Column]()
    for (keyColumn <- keyColumnArray) {
      val index = keyColumn.indexOf(",")
      val sqlName = keyColumn.substring(0, index)
      val dataType = HBaseDataType.withName(keyColumn.substring(index + 1))
      val col = Column(sqlName, null, null, dataType)
      keysList = keysList :+ col
    }
    val rowKey = TypedRowKey(new Columns(keysList))

    val fullHBaseName =
      if (namespace.length == 0) {
        TableName.valueOf(hbaseName)
      }
      else {
        TableName.valueOf(namespace, hbaseName)
      }

    HBaseCatalogTable(tableName, fullHBaseName, rowKey,
      columnFamilies,
      new Columns(columnList),
      HBaseUtils.getPartitions(fullHBaseName, configuration))
  }

  def createMetadataTable(admin: HBaseAdmin) = {
    val desc = new HTableDescriptor(TableName.valueOf(MetaData))
    val coldef = new HColumnDescriptor(ColumnFamily)
    desc.addFamily(coldef)
    admin.createTable(desc)
  }

  def checkTableExists(hbaseTableName: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    admin.tableExists(hbaseTableName)
  }

  def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }


  def createTable(namespace: String, tableName: String,
                  hbaseTableName: String,
                  keyColumns: Seq[KeyColumn],
                  nonKeyColumns: Columns
                  ): Unit = {
    val admin = new HBaseAdmin(configuration)
    val avail = admin.isTableAvailable(MetaData)

    if (!avail) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    table.setAutoFlushTo(false)
    val rowKey = namespace + "." + tableName

    val get = new Get(Bytes.toBytes(rowKey))
    if (table.exists(get)) {
      throw new Exception("row key exists")
    }
    else {
      val put = new Put(Bytes.toBytes(rowKey))

      val result1 = new StringBuilder
      for (column <- nonKeyColumns.columns) {
        val sqlName = column.sqlName
        val family = column.family
        val qualifier = column.qualifier
        val dataType = column.dataType
        result1.append(sqlName)
        result1.append(",")
        result1.append(family)
        result1.append(",")
        result1.append(qualifier)
        result1.append(",")
        result1.append(dataType)
        result1.append(";")
      }
      put.add(ColumnFamily, QualNonKeyColumns, Bytes.toBytes(result1.toString))

      val result2 = new StringBuilder
      result2.append(hbaseTableName)
      put.add(ColumnFamily, QualHbaseName, Bytes.toBytes(result2.toString))

      val result3 = new StringBuilder
      for (column <- keyColumns) {
        val sqlName = column.sqlName
        val dataType = column.dataType
        result3.append(sqlName)
        result3.append(",")
        result3.append(dataType)
        result3.append(";")
      }
      put.add(ColumnFamily, QualKeyColumns, Bytes.toBytes(result3.toString))

      table.put(put)

      table.flushCommits()
    }
  }

}

object HBaseCatalog {
  import org.apache.spark.sql.catalyst.types._

  val MetaData = "metadata"
  val ColumnFamily = Bytes.toBytes("colfam")
  val QualKeyColumns = Bytes.toBytes("keyColumns")
  val QualNonKeyColumns = Bytes.toBytes("nonKeyColumns")
  val QualHbaseName = Bytes.toBytes("hbaseName")

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
  }

  def convertToBytes(dataType: DataType, data: Any): Array[Byte] = {
    dataType match {
      case StringType => Bytes.toBytes(data.asInstanceOf[String])
      case FloatType => Bytes.toBytes(data.asInstanceOf[Float])
      case IntegerType => Bytes.toBytes(data.asInstanceOf[Int])
      case ByteType => Array(data.asInstanceOf[Byte])
      case ShortType => Bytes.toBytes(data.asInstanceOf[Short])
      case DoubleType => Bytes.toBytes(data.asInstanceOf[Double])
      case LongType => Bytes.toBytes(data.asInstanceOf[Long])
      case BinaryType => Bytes.toBytesBinary(data.asInstanceOf[String])
      case BooleanType => Bytes.toBytes(data.asInstanceOf[Boolean])
      case DecimalType => Bytes.toBytes(data.asInstanceOf[BigDecimal])
      case TimestampType => throw new Exception("not supported")
      case _ => throw new Exception("not supported")
    }
  }

  def convertType(dataType: HBaseDataType.Value) : DataType = {
    import HBaseDataType._
    dataType match {
      case STRING => StringType
      case BYTE => ByteType
      case SHORT => ShortType
      case INTEGER => IntegerType
      case LONG => LongType
      case FLOAT => FloatType
      case DOUBLE => DoubleType
      case BOOLEAN => BooleanType
    }
  }

  class Columns(val columns: Seq[Column]) {
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

  case class HBaseCatalogTable(tablename: String,
                               hbaseTableName: TableName,
                               rowKey: TypedRowKey,
                               colFamilies: Set[String],
                               columns: Columns,
                               partitions: Seq[HBasePartition]) {
    val rowKeyParser = RowKeyParser

    val rowKeyColumns = rowKey.columns
  }

  case class TypedRowKey(columns: Columns) extends RowKey

  case object RawBytesRowKey extends RowKey
}

