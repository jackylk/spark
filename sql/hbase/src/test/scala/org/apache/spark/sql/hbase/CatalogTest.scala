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
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark._
import org.apache.spark.sql.catalyst.types.{BooleanType, FloatType, IntegerType, StringType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Ignore}

/**
 * Created by mengbo on 10/2/14.
 */
//@Ignore
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
    catalog = new HBaseCatalog(hbaseContext)
    configuration = HBaseConfiguration.create()
  }

  test("Create Table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"
    val family1 = "family1"
    val family2 = "family2"

    if (!catalog.checkHBaseTableExists(hbaseTableName)) {
      val admin = new HBaseAdmin(configuration)
      val desc = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      desc.addFamily(new HColumnDescriptor(family1))
      desc.addFamily(new HColumnDescriptor(family2))
      admin.createTable(desc)
    }

    var allColumns = List[KeyColumn]()
    allColumns = allColumns :+ KeyColumn("column2", IntegerType)
    allColumns = allColumns :+ KeyColumn("column1", StringType)
    allColumns = allColumns :+ KeyColumn("column4", FloatType)
    allColumns = allColumns :+ KeyColumn("column3", BooleanType)

    val keyColumn1 = KeyColumn("column1", StringType)
    val keyColumn2 = KeyColumn("column2", IntegerType)
    var keyColumns = List[KeyColumn]()
    keyColumns = keyColumns :+ keyColumn1
    keyColumns = keyColumns :+ keyColumn2

    val nonKeyColumn3 = NonKeyColumn("column3", BooleanType, family1, "qualifier1")
    val nonKeyColumn4 = NonKeyColumn("column4", FloatType, family2, "qualifier2")
    var nonKeyColumns = List[NonKeyColumn]()
    nonKeyColumns = nonKeyColumns :+ nonKeyColumn3
    nonKeyColumns = nonKeyColumns :+ nonKeyColumn4

    catalog.createTable(tableName, namespace, hbaseTableName, allColumns, keyColumns, nonKeyColumns)
  }

  test("Get Table") {
    // prepare the test data
    val hbaseNamespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val oresult = catalog.getTable(tableName)
    assert(oresult.isDefined)
    val result = oresult.get
    assert(result.tableName === tableName)
    assert(result.hbaseNamespace === hbaseNamespace)
    assert(result.hbaseTableName === hbaseTableName)
    assert(result.keyColumns.size === 2)
    assert(result.nonKeyColumns.size === 2)
    assert(result.allColumns.size === 4)

    // check the data type
    assert(result.keyColumns(0).dataType === StringType)
    assert(result.keyColumns(1).dataType === IntegerType)
    assert(result.nonKeyColumns(0).dataType === BooleanType)
    assert(result.nonKeyColumns(1).dataType === FloatType)

    val relation = catalog.lookupRelation(None, tableName)
    val hbRelation = relation.asInstanceOf[HBaseRelation]
    assert(hbRelation.nonKeyColumns.map(_.family) == List("family1", "family2"))
    val keyColumns = Seq(KeyColumn("column1", StringType), KeyColumn("column2", IntegerType))
    assert(hbRelation.keyColumns.equals(keyColumns))
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
