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

class JoinsSuite extends QueriesSuiteBase {

  private val logger = Logger.getLogger(getClass.getName)

  var testnm = "Basic Join"
  test(testnm) {
    val allcols = (1 to 3).foldLeft("") { case (cols, px) =>
      val tn = s"t$px"
        val newcols =   s"""$tn.intcol ${tn}intcol, $tn.floatcol ${tn}floatcol, $tn.strcol ${tn}strcol, max($tn.bytecol)  ${tn}bytecol,
         | max($tn.shortcol)  ${tn}shortcol,
         | max($tn.floatcol)  ${tn}floatcolmax, max($tn.doublecol)  ${tn}doublecol,
         | max($tn.longcol)  ${tn}longcol"""
      s"""$cols,
         |$newcols
       """.stripMargin
    }
    val query1 =
      s"""select count(1) as cnt,
         |$allcols
         | from $tabName
         |  where t1strcol like '%Row%' and t2.shortcol < 12345 and t3.doublecol > 5678912.345681
         |  and t3.doublecol < 5678912.345684
         | group by t1intcol, t2floatcol, t3strcol order by t1strcol desc"""
        .stripMargin

    val execQuery1 = hbc.executeSql(query1)
    val result1 = execQuery1.toRdd.collect()
    assert(result1.size == 2, s"$testnm failed on size")
    val exparr = Array(
      Array(1,23456783, 45657.83F, "Row3", 'c', 12343, 45657.83F, 5678912.345683, 3456789012343L),
      Array(1,23456782, 45657.82F, "Row2", 'b', 12342, 45657.82F, 5678912.345682, 3456789012342L))

    var res = {
      for (rx <- 0 until exparr.size)
      yield compareWithTol(result1(rx).toSeq, exparr(rx), s"Row$rx failed")
    }.foldLeft(true) { case (res1, newres) => res1 && newres}
    assert(res, "One or more rows did not match expected")

    println(s"$query1 came back with ${result1.size} results")
    println(result1.mkString)

    println(s"Test $testnm completed successfully")
  }

}

