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
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.hbase.HBaseCatalog.{Column, Columns, HBaseDataType, KeyColumn}
import org.apache.spark.{Logging, SparkContext, _}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by mengbo on 10/2/14.
 */
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
    configuration = HBaseConfiguration.create()
    catalog = new HBaseCatalog(hbaseContext, configuration)
  }

  test("create table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val keyColumn1 = KeyColumn("column1", HBaseDataType.STRING)
    val keyColumn2 = KeyColumn("column2", HBaseDataType.INTEGER)
    var keyColumns = List[KeyColumn]()
    keyColumns = keyColumns :+ keyColumn1
    keyColumns = keyColumns :+ keyColumn2

    val nonKeyColumn3 = Column("column3", "family1", "qualifier1", HBaseDataType.BOOLEAN)
    val nonKeyColumn4 = Column("column4", "family2", "qualifier2", HBaseDataType.FLOAT)
    var nonKeyColumnList = List[Column]()
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn3
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn4
    val nonKeyColumns = new Columns(nonKeyColumnList)

    catalog.createTable(namespace, tableName, hbaseTableName, keyColumns, nonKeyColumns)
  }

  test("get table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val oresult = catalog.getTable(Some(namespace), tableName)
    assert(oresult.isDefined)
    val result = oresult.get
    assert(result.tablename === tableName)
    assert(result.hbaseTableName.tableName.getNameAsString === namespace + ":" + hbaseTableName)
    assert(result.colFamilies.size === 2)
    assert(result.columns.columns.size === 2)
  }
}
