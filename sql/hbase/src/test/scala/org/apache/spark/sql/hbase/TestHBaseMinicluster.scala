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

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest.FunSuite

class TestHBaseMinicluster extends FunSuite{

  test("test for hbase minicluster") {
    val testUtil = new HBaseTestingUtility
    val cluster = testUtil.startMiniCluster()
    val hbaseAdmin = new HBaseAdmin(testUtil.getConfiguration)
    println(hbaseAdmin.tableExists("wf"))
  }

}
