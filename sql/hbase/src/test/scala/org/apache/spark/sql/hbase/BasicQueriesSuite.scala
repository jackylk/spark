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

import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfterEach, ConfigMap, FunSuiteLike}

class BasicQueriesSuite extends HBaseIntegrationTestBase with CreateTableAndLoadData {
  self : HBaseIntegrationTestBase =>

 override protected def beforeAll(configMap: ConfigMap): Unit = {
   super.beforeAll(configMap)
    createTableAndLoadData(hbc)
  }

  val tabName = DefaultTableName
  
  private val logger = Logger.getLogger(getClass.getName)

  val CompareTol = 1e-6
  def compareWithTol(actarr: Seq[Any], exparr: Seq[Any], emsg: String): Boolean = {
    actarr.zip(exparr).forall { case (a,e) =>
      (a, e) match {
        case (a, e) =>
          val eq = if (a.isInstanceOf[Float] || a.isInstanceOf[Double]) {
                   Math.abs(a.asInstanceOf[Double]-e.asInstanceOf[Double]) < CompareTol
          } else {
            a == e
          }
          if (!eq) {
            logger.error(s"$emsg: Mismatch- act=$a exp=$e")
          }
          eq
        case _ => throw new IllegalArgumentException("Expected tuple")
      }
    }
  }

  var testnm = "StarOperator * with limit"
  test(testnm) {
    val query1 =
      s"""select * from $tabName limit 3"""
        .stripMargin

    val execQuery1 = hbc.executeSql(query1)
    val result1 = execQuery1.toRdd.collect()
    assert(result1.size == 3,s"$testnm failed on size")
    val exparr = Array(Array("Row1",'a',12345,23456789,3456789012345L,45657.89, 5678912.345678),
      Array("Row2",'b',12342,23456782,3456789012342L,45657.82, 5678912.345682),
Array("Row3",'c',12343,23456783,3456789012343L,45657.83, 5678912.345683))

    val res = {
      for (rx <- 0 until 3)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(false){ case (res, newres) => (res && newres)}

    assert(res,"One or more rows did not match expected")

    println(s"Select * from $tabName limit 3 came back with ${result1.size} results")
    println(result1.mkString)

    val sql2 =
      s"""select * from $tabName limit 2"""
        .stripMargin

    val executeSql2 = hbc.executeSql(sql2)
    val results = executeSql2.toRdd.collect()
    println(s"Select * from $tabName limit 2 came back with ${results.size} results")
    assert(results.size == 2, s"$testnm failed assertion on size")
    assert(results.mkString(",").equals("[row4,4,8],[row5,5,10]"), s"$testnm failed assertion on compare")
    println(results.mkString)

    println("Test load data into HBase completed successfully")
  }

  testnm = "All fields * query with limit"
  test(testnm) {
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