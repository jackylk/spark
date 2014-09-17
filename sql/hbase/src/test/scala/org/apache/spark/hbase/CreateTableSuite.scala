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


import org.apache.spark.sql.QueryTest

/* Implicits */
import org.apache.spark.sql.hbase.TestHbase._

class CreateTableSuite extends QueryTest  {
  TestData // Initialize TestData

  test("create table") {
    sql("CREATE TABLE tableName (col1 TYPE1, col2 TYPE2, col3 TYPE3, col4 TYPE4, col5 TYPE5, col6 TYPE6, col7 TYPE7) " +
      "MAPPED BY (hbaseTableName, KEYS=[col7, col1, col3], COLS=[cf1.cq11=col2, cf1.cq12=col4, cf2.cq21=col5, cf2.cq22=col6])")
  }

  test("SPARK-3176 Added Parser of SQL ABS()") {
    checkAnswer(
      sql("SELECT ABS(-1.3)"),
      1.3)
  }
}
