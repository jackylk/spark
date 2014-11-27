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

import org.scalatest.{BeforeAndAfterEach, ConfigMap, FunSuiteLike}

class BasicQueriesSuite extends HBaseIntegrationTestBase with CreateTableAndLoadData {
  self : HBaseIntegrationTestBase =>

 override protected def beforeAll(configMap: ConfigMap): Unit = {
   super.beforeAll(configMap)
    createTableAndLoadData(hbc)
  }

  val tabName = DefaultTableName
  
  test("StarOperator * with limit") {
    val query1 =
      s"""select * from $tabName limit 3"""
        .stripMargin

    val execQuery1 = hbc.executeSql(query1)
    val result1 = execQuery1.toRdd.collect()
    result1.foreach(println)
    assert(result1.size == 3)
    assert(result1.mkString(",").equals("[row4,4,8],[row5,5,10],[row6,6,12]"))
    println(s"Select * from $tabName limit 3 came back with ${result1.size} results")
    println(result1.mkString)

    val sql2 =
      s"""select * from $tabName limit 2"""
        .stripMargin

    val executeSql2 = hbc.executeSql(sql2)
    val results = executeSql2.toRdd.collect()
    println(s"Select * from $tabName limit 2 came back with ${results.size} results")
    assert(results.size == 2)
    assert(results.mkString(",").equals("[row4,4,8],[row5,5,10]"))
    println(results.mkString)

    println("Test load data into HBase completed successfully")
  }

  test("All fields * query with limit") {
    val query1 =
      s"""select * from $tabName limit 3"""
        .stripMargin

    val execQuery1 = hbc.executeSql(query1)
    val result1 = execQuery1.toRdd.collect()
    assert(result1.size == 3)
    assert(result1.mkString(",").equals("[row4,4,8],[row5,5,10],[row6,6,12]"))
    println(s"Select * from $tabName limit 3 came back with ${result1.size} results")
    println(result1.mkString)

    val sql2 =
      s"""select * from $tabName limit 2"""
        .stripMargin

    val executeSql2 = hbc.executeSql(sql2)
    val results = executeSql2.toRdd.collect()
    println(s"Select * from $tabName limit 2 came back with ${results.size} results")
    assert(results.size == 2)
    assert(results.mkString(",").equals("[row4,4,8],[row5,5,10]"))
    println(results.mkString)

    println("Test load data into HBase completed successfully")
  }

}