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

import org.apache.hadoop.hbase.HTableDescriptor
import org.scalatest.FunSuiteLike

class BulkLoadIntoTableIntSuite extends HBaseIntegrationTestBase with FunSuiteLike {

  // Change from ignore to test to run this. TODO Presently there is a bug in create table
  // that the original testcase writers (Wangei ?)  need to fix

  val TableName = "TestTable"

  test("load data into hbase") {
    // this need to local test with hbase, so here to ignore this

    val descriptor = new HTableDescriptor(s2b(TableName))
    hbaseAdmin.createTable(descriptor)
    println(s"Created table $TableName: " +
      s"isTableAvailable= ${hbaseAdmin.isTableAvailable(s2b(TableName))}" +
      s" tableDescriptor= ${hbaseAdmin.getTableDescriptor(s2b(TableName))}")

    val drop = "drop table testblk"
    val executeSql0 = hbc.executeSql(drop)
    try {
      executeSql0.toRdd.collect().foreach(println)
      println(s"Dropped table $TableName")
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        logger.error(e.getMessage)
        println(s"Drop table failed $TableName")
    }

    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY (col1)) MAPPED BY (wf, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val executeSql1 = hbc.executeSql(sql1)
    executeSql1.toRdd.collect().foreach(println)

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH '/shared/hwspark/sql/hbase/src/test/resources/loadData.csv' INTO TABLE testblk"
    val result3 = hbc.executeSql(loadSql).toRdd.collect()

    val query1 =
      s"""select * from testblk limit 3"""
        .stripMargin

    val execQuery1 = hbc.executeSql(query1)
    val result1 = execQuery1.toRdd.collect()
    assert(result1.size == 3)
    assert(result1.mkString(",").equals("[row4,4,8],[row5,5,10],[row6,6,12]"))
    println(s"Select * from testblk limit 3 came back with ${result1.size} results")
    println(result1.mkString)

    val sql2 =
      s"""select * from testblk limit 2"""
        .stripMargin

    val executeSql2 = hbc.executeSql(sql2)
    val results = executeSql2.toRdd.collect()
    println(s"Select * from testblk limit 2 came back with ${results.size} results")
    assert(results.size == 2)
    assert(results.mkString(",").equals("[row4,4,8],[row5,5,10]"))
    println(results.mkString)

    println("Test load data into HBase completed successfully")
  }
}