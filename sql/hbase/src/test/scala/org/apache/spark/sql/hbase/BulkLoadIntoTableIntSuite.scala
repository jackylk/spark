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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hbase.logical.LoadDataIntoTable
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.hbase.execution.BulkLoadIntoTable
import org.apache.hadoop.hbase.util.Bytes

class BulkLoadIntoTableIntSuite extends HBaseIntegrationTestBase {


  override def beforeAll: Unit = {
    super.beforeAll
  }

  // Change from ignore to test to run this. TODO Presently there is a bug in create table
  // that the original testcase writers (Wangei ?)  need to fix
  test("load data into hbase") {
    // this need to local test with hbase, so here to ignore this
    // create sql table map with hbase table and run simple sql

    val drop = "drop table testblk"
    val executeSql0 = hbc.executeSql(drop)
    try {
      executeSql0.toRdd.collect().foreach(println)
    } catch {
      case e: IllegalStateException =>
        // do not throw exception here
        logWarning(e.getMessage)
    }

    """CREATE TABLE tableName (col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
      col5 LONG, col6 FLOAT, col7 DOUBLE, PRIMARY KEY(col7, col1, col3))
      MAPPED BY (hbaseTableName1, COLS=[col2=cf1.cq11,
      col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""

    val sql1 =
      s"""CREATE TABLE testblk(col1 STRING, col2 STRING, col3 STRING, PRIMARY KEY(col1))
          MAPPED BY (wf, COLS=[col2=cf1.a, col3=cf1.b])"""
        .stripMargin

    val sql2 =
      s"""select * from testblk limit 5"""
        .stripMargin

    val executeSql1 = hbc.executeSql(sql1)
    executeSql1.toRdd.collect().foreach(println)

    val executeSql2 = hbc.executeSql(sql2)
    executeSql2.toRdd.collect().foreach(println)

    // then load data into table
    val loadSql = "LOAD DATA LOCAL INPATH './sql/hbase/src/test/resources/loadData.csv' INTO TABLE testblk"

    val executeSql3 = hbc.executeSql(loadSql)
    executeSql3.toRdd.collect().foreach(println)
    hbc.sql("select * from testblk").collect().foreach(println)
  }
}