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
