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
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.catalyst.types.{FloatType, BooleanType, IntegerType, StringType}
import org.apache.spark.sql.hbase.HBaseCatalog.{Column, Columns, KeyColumn}
import org.apache.spark.{Logging, SparkContext, _}
import org.scalatest.{Ignore, BeforeAndAfterAll, FunSuite}

/**
 * Created by mengbo on 10/2/14.
 */
@Ignore
class CatalogTest extends FunSuite with BeforeAndAfterAll with Logging {
  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var hbaseContext: HBaseSQLContext = _
  var configuration: Configuration = _
  var catalog: HBaseCatalog = _

  override def beforeAll() = {
    sparkConf = new SparkConf().setAppName("Catalog Test").setMaster("local[4]")
    sparkContext = new SparkContext(sparkConf)
    hbaseContext = new HBaseSQLContext(sparkContext)
    configuration = hbaseContext.configuration
    catalog = new HBaseCatalog(hbaseContext, configuration)
  }

  test("Create Table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"
    val family1 = "family1"
    val family2 = "family2"

    val admin = new HBaseAdmin(configuration)
    val desc = new HTableDescriptor(TableName.valueOf(hbaseTableName))
    desc.addFamily(new HColumnDescriptor(family1))
    desc.addFamily(new HColumnDescriptor(family2))
    admin.createTable(desc)

    val keyColumn1 = KeyColumn("column1", StringType)
    val keyColumn2 = KeyColumn("column2", IntegerType)
    var keyColumns = List[KeyColumn]()
    keyColumns = keyColumns :+ keyColumn1
    keyColumns = keyColumns :+ keyColumn2

    val nonKeyColumn3 = Column("column3", family1, "qualifier1", BooleanType)
    val nonKeyColumn4 = Column("column4", family2, "qualifier2", FloatType)
    var nonKeyColumnList = List[Column]()
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn3
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn4
    val nonKeyColumns = new Columns(nonKeyColumnList)

    catalog.createTable(namespace, tableName, hbaseTableName, keyColumns, nonKeyColumns)
  }

  test("Get Table") {
    // prepare the test data
    val hbaseNamespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val oresult = catalog.getTable(tableName)
    assert(oresult.isDefined)
    val result = oresult.get
    assert(result.tablename === tableName)
    assert(result.hbaseTableName.tableName.getNameAsString === hbaseNamespace + ":" + hbaseTableName)
    assert(result.colFamilies.size === 2)
    assert(result.columns.columns.size === 2)

    // check the data type
    assert(result.rowKey.columns.columns(0).dataType === StringType)
    assert(result.rowKey.columns.columns(1).dataType === IntegerType)
    assert(result.columns.columns(0).dataType === BooleanType)
    assert(result.columns.columns(1).dataType === FloatType)

    val relation = catalog.lookupRelation(None, tableName)
    val hbRelation = relation.asInstanceOf[HBaseRelation]
    assert(hbRelation.colFamilies == Set("family1", "family2"))
    assert(hbRelation.partitionKeys == Seq("column1", "column2"))
    val rkColumns = new Columns(Seq(Column("column1", null, "column1", StringType, 1),
      Column("column1", null, "column1", IntegerType, 2)))
    assert(hbRelation.catalogTable.rowKeyColumns.equals(rkColumns))
    assert(relation.childrenResolved)
  }

  test("Delete Table") {
    // prepare the test data
    val tableName = "testTable"

    catalog.deleteTable(tableName)
  }

  test("Check Logical Table Exist") {
    val tableName = "non-exist"

    assert(catalog.checkLogicalTableExist(tableName) === false)
  }
}
