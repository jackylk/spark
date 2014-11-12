
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

/**
 * HBaseIntegrationTestBase
 *
 */
package org.apache.spark.sql.hbase

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, MiniHBaseCluster}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{FunSuite, BeforeAndAfterAll, Suite}

/**
 * HBaseTestSparkContext used for test.
 *
 */
trait HBaseIntegrationTestBase extends FunSuite with BeforeAndAfterAll { self: Suite =>

  @transient var sc: SparkContext = _

  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbaseAdmin: HBaseAdmin = null
  @transient var hbc: HBaseSQLContext = null
  @transient var catalog: HBaseCatalog = null
  @transient var testUtil: HBaseTestingUtility = null

  @transient val logger = Logger.getLogger(getClass.getName)

  def sparkContext: SparkContext = sc

  val useMiniCluster: Boolean = true

  val NMasters = 1
  val NRegionServers = 1
  // 3
  val NDataNodes = 0

  val NWorkers = 1

  val startTime = (new Date).getTime

  override def beforeAll: Unit = {
    ctxSetup
  }

  def ctxSetup() {
    logger.info(s"Setting up context with useMiniCluster=$useMiniCluster")
    if (useMiniCluster) {
      logger.info(s"Spin up hbase minicluster w/ $NMasters mast, $NRegionServers RS, $NDataNodes dataNodes")
      testUtil = new HBaseTestingUtility
      config = testUtil.getConfiguration
    } else {
      config = HBaseConfiguration.create
    }
    //    cluster = HBaseTestingUtility.createLocalHTU.
    //      startMiniCluster(NMasters, NRegionServers, NDataNodes)
    //    config = HBaseConfiguration.create
    config.set("hbase.regionserver.info.port", "-1")
    config.set("hbase.master.info.port", "-1")
    config.set("dfs.client.socket-timeout", "240000")
    config.set("dfs.datanode.socket.write.timeout", "240000")
    config.set("zookeeper.session.timeout", "240000")
    config.set("zookeeper.minSessionTimeout", "10")
    config.set("zookeeper.tickTime", "10")
    config.set("hbase.rpc.timeout", "240000")
    config.set("ipc.client.connect.timeout", "240000")
    config.set("dfs.namenode.stale.datanode.interva", "240000")
    config.set("hbase.rpc.shortoperation.timeout", "240000")
//    config.set("hbase.regionserver.lease.period", "240000")

    if (useMiniCluster) {
      cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
      println(s"# of region servers = ${cluster.countServedRegions}")
    }

    @transient val conf = new SparkConf
    val SparkPort = 11223
    conf.set("spark.ui.port", SparkPort.toString)
    //    @transient val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext", conf)
    hbc = new HBaseSQLContext(TestSQLContext.sparkContext, Some(config))
    import collection.JavaConverters._
    config.iterator.asScala.foreach { entry =>
      hbc.setConf(entry.getKey, entry.getValue)
    }
    catalog = hbc.catalog
    hbaseAdmin = new HBaseAdmin(config)
  }

  override def afterAll: Unit = {
    logger.info(s"Test ${getClass.getName} completed at ${(new java.util.Date).toString} duration=${((new java.util.Date).getTime - startTime)/1000}")
    hbc.sparkContext.stop()
    hbc = null
  }
}
