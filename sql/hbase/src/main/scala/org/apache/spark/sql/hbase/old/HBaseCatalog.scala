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
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

/**
 * HBaseCatalog
 */
private[hbase] class HBaseCatalog(@transient hbaseContext: HBaseSQLContext,
                                  @transient configuration: Configuration)
  extends SimpleCatalog(false) with Logging with Serializable {

  import org.apache.spark.sql.hbase.HBaseCatalog._

  @transient val logger = Logger.getLogger(getClass.getName)

  override def registerTable(databaseName: Option[String], tableName: String,
                             plan: LogicalPlan): Unit = ???

  // TODO(Bo): read the entire HBASE_META_TABLE and process it once, then cache it
  // in this class
  override def unregisterAllTables(): Unit = {
    tables.clear
  }

  override def unregisterTable(databaseName: Option[String], tableName: String): Unit =
    tables -= tableName

  override def lookupRelation(nameSpace: Option[String], sqlTableName: String,
                              alias: Option[String]): LogicalPlan = {
    val itableName = processTableName(sqlTableName)
    val catalogTable = getTable(sqlTableName)
    if (catalogTable.isEmpty) {
      throw new IllegalArgumentException(
        s"Table $nameSpace.$sqlTableName does not exist in the catalog")
    }
    val tableName = TableName.valueOf(nameSpace.orNull, itableName)
    new HBaseRelation(configuration, hbaseContext, catalogTable.get)
  }

  protected def processTableName(tableName: String): String = {
    if (!caseSensitive) {
      tableName.toLowerCase
    } else {
      tableName
    }
  }

  def getDataType(dataType: String): DataType = {
    if (dataType.equalsIgnoreCase(StringType.simpleString)) {
      StringType
    } else if (dataType.equalsIgnoreCase(ByteType.simpleString)) {
      ByteType
    } else if (dataType.equalsIgnoreCase(ShortType.simpleString)) {
      ShortType
    } else if (dataType.equalsIgnoreCase(IntegerType.simpleString)) {
      IntegerType
    } else if (dataType.equalsIgnoreCase(LongType.simpleString)) {
      LongType
    } else if (dataType.equalsIgnoreCase(FloatType.simpleString)) {
      FloatType
    } else if (dataType.equalsIgnoreCase(DoubleType.simpleString)) {
      DoubleType
    } else if (dataType.equalsIgnoreCase(BooleanType.simpleString)) {
      BooleanType
    } else {
      throw new IllegalArgumentException(s"Unrecognized data type '${dataType}'")
    }
  }

  def getTable(tableName: String): Option[HBaseCatalogTable] = {
    val table = new HTable(configuration, MetaData)

    val get = new Get(Bytes.toBytes(tableName))
    val rest1 = table.get(get)
    if (rest1 == null) {
      None
    } else {
      var columnList = List[Column]()
      import scala.collection.mutable.{Seq => MutSeq}
      var columnFamilies = MutSeq[(String)]()

      var nonKeyColumns = Bytes.toString(rest1.getValue(ColumnFamily, QualNonKeyColumns))
      if (nonKeyColumns != null) {
        if (nonKeyColumns.length > 0) {
          nonKeyColumns = nonKeyColumns.substring(0, nonKeyColumns.length - 1)
        }

        val nonKeyColumnArray = nonKeyColumns.split(";")
        for (nonKeyColumn <- nonKeyColumnArray) {
          val nonKeyColumnInfo = nonKeyColumn.split(",")
          val sqlName = nonKeyColumnInfo(0)
          val family = nonKeyColumnInfo(1)
          val qualifier = nonKeyColumnInfo(2)
          val dataType = getDataType(nonKeyColumnInfo(3))

          val column = Column(sqlName, family, qualifier, dataType)
          columnList = columnList :+ column
          if (!(columnFamilies contains family)) {
            columnFamilies = columnFamilies :+ family
          }
        }
      }

      // What if this were not an HBase table?  We get NPE's here..
      val hbaseName = Bytes.toString(rest1.getValue(ColumnFamily, QualHbaseName))
      val hbaseNameArray = hbaseName.split(",")
      val hbaseNamespace = hbaseNameArray(0)
      val hbaseTableName = hbaseNameArray(1)

      var keyColumns = Bytes.toString(rest1.getValue(ColumnFamily, QualKeyColumns))
      if (keyColumns.length > 0) {
        keyColumns = keyColumns.substring(0, keyColumns.length - 1)
      }
      val keyColumnArray = keyColumns.split(";")
      var keysList = List[Column]()
      for (keyColumn <- keyColumnArray) {
        val index = keyColumn.indexOf(",")
        val sqlName = keyColumn.substring(0, index)
        val dataType = getDataType(keyColumn.substring(index + 1))
        val qualName = sqlName
        val col = Column(sqlName, null, qualName, dataType)
        keysList = keysList :+ col
      }
      val rowKey = new Columns(keysList)

      val fullHBaseName =
        if (hbaseNamespace.length == 0) {
          TableName.valueOf(hbaseTableName)
        }
        else {
          TableName.valueOf(hbaseNamespace, hbaseTableName)
        }

      Some(HBaseCatalogTable(tableName,
        SerializableTableName(fullHBaseName),
        rowKey,
        Seq(columnFamilies: _*),
        new Columns(columnList)))
    }
  }

  def createMetadataTable(admin: HBaseAdmin) = {
    val desc = new HTableDescriptor(TableName.valueOf(MetaData))
    val coldef = new HColumnDescriptor(ColumnFamily)
    desc.addFamily(coldef)
    admin.createTable(desc)
  }

  def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    admin.tableExists(hbaseTableName)
  }

  def checkLogicalTableExist(tableName: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    if (!checkHBaseTableExists(MetaData)) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    val get = new Get(Bytes.toBytes(tableName))
    val result = table.get(get)

    result.size() > 0
  }

  def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    val admin = new HBaseAdmin(configuration)
    val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }

  def deleteTable(tableName: String): Unit = {
    if (!checkLogicalTableExist(tableName)) {
      throw new Exception("The logical table:" +
         tableName + " doesn't exist")
    }

    val admin = new HBaseAdmin(configuration)
    val table = new HTable(configuration, MetaData)

    val delete = new Delete(Bytes.toBytes(tableName))
    table.delete(delete)

    table.close()
  }

  def createTable(hbaseNamespace: String,
                  tableName: String,
                  hbaseTableName: String,
                  keyColumns: Seq[KeyColumn],
                  nonKeyColumns: Columns
                   ): Unit = {
    if (checkLogicalTableExist(tableName)) {
      throw new Exception("The logical table:" +
        tableName + " has already existed")
    }

    if (!checkHBaseTableExists(hbaseTableName)) {
      throw new Exception("The HBase table " +
        hbaseTableName + " doesn't exist")
    }

    nonKeyColumns.columns.foreach {
      case Column(_, family, _, _, _) =>
        if (!checkFamilyExists(hbaseTableName, family)) {
          throw new Exception(
            "The HBase table doesn't contain the Column Family: " +
              family)
        }
    }

    val admin = new HBaseAdmin(configuration)
    val avail = admin.isTableAvailable(MetaData)

    if (!avail) {
      // create table
      createMetadataTable(admin)
    }

    val table = new HTable(configuration, MetaData)
    table.setAutoFlushTo(false)
    val rowKey = tableName

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
        result1.append(dataType.simpleString)
        result1.append(";")
      }
      put.add(ColumnFamily, QualNonKeyColumns, Bytes.toBytes(result1.toString))

      val result2 = new StringBuilder
      result2.append(hbaseNamespace)
      result2.append(",")
      result2.append(hbaseTableName)
      put.add(ColumnFamily, QualHbaseName, Bytes.toBytes(result2.toString))

      val result3 = new StringBuilder
      for (column <- keyColumns) {
        val sqlName = column.sqlName
        val dataType = column.dataType
        result3.append(sqlName)
        result3.append(",")
        result3.append(dataType.simpleString)
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

  case class Column(sqlName: String, family: String, qualifier: String,
                    dataType: DataType,
                    ordinal: Int = -1) extends Ordered[Column] {
    def fullName = s"$family:$qualifier"

    def toColumnName = ColumnName(Some(family), qualifier)

    override def hashCode(): Int = {
      sqlName.hashCode * 31 + (if (family != null) family.hashCode * 37 else 0) +
        qualifier.hashCode * 41 + dataType.hashCode * 43 + ordinal.hashCode * 47
    }

    override def equals(obj: scala.Any): Boolean = {
      val superEquals = super.equals(obj)
      val retval = hashCode == obj.hashCode
      retval // note: superEquals is false whereas retval is true. Interesting..
    }

    override def compare(that: Column): Int = {
      -(ordinal - that.ordinal)
    }
  }

  object Column extends Serializable {
    def toAttributeReference(col: Column): AttributeReference = {
      AttributeReference(col.sqlName, col.dataType,
        nullable = true)()
    }
  }

  class Columns(inColumns: Seq[Column]) extends Serializable {
    private val colx = new java.util.concurrent.atomic.AtomicInteger

    val columns = inColumns.map {
      case Column(s, f, q, d, -1) => Column(s, f, q, d, nextOrdinal)
      case col => col
    }

    def nextOrdinal() = colx.getAndIncrement

    def apply(colName: ColumnName) = {
      map(colName)
    }

    def apply(colName: String): Option[Column] = {
      val Pat = "(.*):(.*)".r
      colName match {
        case Pat(colfam, colqual) => toOpt(map(ColumnName(Some(colfam), colqual)))
        case sqlName: String => findBySqlName(sqlName)
      }
    }

    def toOpt[A: reflect.ClassTag](a: A): Option[A] = a match {
      case a: Some[A] => a
      case None => None
      case a: A => Some(a)
    }

    def findBySqlName(sqlName: String): Option[Column] = {
      map.iterator.find { case (cname, col) =>
        col.sqlName == sqlName
      }.map(_._2)
    }

    def toColumnNames() = {
      columns.map(_.toColumnName)
    }

    import scala.collection.mutable

    private val map: mutable.Map[ColumnName, Column] =
      columns.foldLeft(mutable.Map[ColumnName, Column]()) { case (m, c) =>
        m(ColumnName(if (c.family != null) Some(c.family) else None,
          c.qualifier)) = c
        m
      }

    def getColumn(colName: String): Option[Column] = map.get(ColumnName(colName))

    def families() = Set(columns.map(_.family))

    def asAttributes() = {
      columns.map { col =>
        Column.toAttributeReference(col)
      }
    }

    override def equals(that: Any) = {
      //      that.isInstanceOf[Columns] && that.hashCode == hashCode
      if (!that.isInstanceOf[Columns]) {
        false
      } else {
        val other = that.asInstanceOf[Columns]
        val result = other.columns.size == columns.size && columns.zip(other.columns)
          .forall { case (col, ocol) =>
          col.equals(ocol)
        }
        result
      }
    }

    override def hashCode() = {
      val hash = columns.foldLeft(47 /* arbitrary start val .. */) {
        _ + _.hashCode
      }
      hash
    }

  }

  case class HBaseCatalogTable(tablename: String,
                               hbaseTableName: SerializableTableName,
                               rowKey: Columns,    // Should do RowKey for geneeralization
                               colFamilies: Seq[String],
                               columns: Columns) {

    val rowKeyColumns = rowKey

    lazy val allColumns = new Columns(rowKeyColumns.columns ++ columns.columns)

  }

  case class KeyColumn(sqlName: String, dataType: DataType)

  // Following supports Pluggable RowKey.
  trait RowKey

  case class TypedRowKey(columns: Columns) extends RowKey

  case object RawBytesRowKey extends RowKey

  // Convenience method to aid in validation/testing
  private[hbase] def getKeysFromAllMetaTableRows(configuration: Configuration)
      : Seq[HBaseRawType] = {
    val htable = new HTable(configuration, MetaData)
    val scan = new Scan
    scan.setFilter(new FirstKeyOnlyFilter())
    val scanner = htable.getScanner(scan)
    import scala.collection.JavaConverters._
    import scala.collection.mutable
    val rkeys = mutable.ArrayBuffer[HBaseRawType]()
    val siter = scanner.iterator.asScala
    while (siter.hasNext) {
      rkeys += siter.next.getRow
    }
    rkeys
  }

}

