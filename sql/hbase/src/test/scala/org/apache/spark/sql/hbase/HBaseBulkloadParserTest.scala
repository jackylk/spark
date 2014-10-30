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
import org.scalatest.{FunSuite, Matchers}

/**
 * Test Suite for HBase bulkload feature
 */

class HBaseBulkloadParserTest extends FunSuite with Matchers {

  // Test if we can parse 'LOAD DATA LOCAL INPATH './usr/file.csv' INTO TABLE tb'
  test("bulkload parser test, local file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA LOCAL INPATH './usr/file.csv' INTO TABLE tb"
    //val sql = "select"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[LoadDataIntoTable])

    val l = plan.asInstanceOf[LoadDataIntoTable]
    assert(l.path.equals(raw"./usr/file.csv"))
    assert(l.isLocal)

    assert(plan.children(0).isInstanceOf[UnresolvedRelation])
    val r = plan.children(0).asInstanceOf[UnresolvedRelation]
    assert(r.tableName.equals("tb"))
  }

  // Test if we can parse 'LOAD DATA INPATH '/usr/hdfsfile.csv' INTO TABLE tb'
  test("bulkload parser test, load hdfs file") {

    val parser = new HBaseSQLParser()
    val sql = raw"LOAD DATA INPATH '/usr/hdfsfile.csv' INTO TABLE tb"
    //val sql = "select"

    val plan: LogicalPlan = parser(sql)
    assert(plan != null)
    assert(plan.isInstanceOf[LoadDataIntoTable])

    val l = plan.asInstanceOf[LoadDataIntoTable]
    assert(l.path.equals(raw"/usr/hdfsfile.csv"))
    assert(!l.isLocal)
    assert(plan.children(0).isInstanceOf[UnresolvedRelation])
    val r = plan.children(0).asInstanceOf[UnresolvedRelation]
    assert(r.tableName.equals("tb"))
  }
}
