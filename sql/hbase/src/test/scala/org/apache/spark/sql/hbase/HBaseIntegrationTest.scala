package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan, HTable, HBaseAdmin}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hbase.HBaseCatalog.{Columns, HBaseDataType, Column}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, MiniHBaseCluster}

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
class HBaseIntegrationTest extends FunSuite
    with HBaseTestingSparkContext with BeforeAndAfterAll {
  override val logger = Logger.getLogger(getClass.getName)

  val NMasters = 1
  val NRegionServers = 3
  val NDataNodes = 0

  var hbContext : HBaseSQLContext = _
  var cluster : MiniHBaseCluster = _
  var config : Configuration = _
  var hbaseAdmin : HBaseAdmin = _
  var catalog : HBaseCatalog = _
  var testUtil :HBaseTestingUtility = _

  override def beforeAll() = {
    super.beforeAll()
    logger.info(s"Spin up hbase minicluster w/ $NMasters mast, $NRegionServers RS, $NDataNodes dataNodes")
    testUtil = new HBaseTestingUtility
//    cluster = HBaseTestingUtility.createLocalHTU.
//      startMiniCluster(NMasters, NRegionServers, NDataNodes)
//    config = HBaseConfiguration.create
    config = testUtil.getConfiguration
    config.set("hbase.regionserver.info.port","-1")
    cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
    println(s"# of region servers = ${cluster.countServedRegions}")
    val sc = new SparkContext("local[1]", "HBaseTestsSparkContext")
    hbContext = new HBaseSQLContext(sc, config)
    catalog = hbContext.catalog
    hbaseAdmin = new HBaseAdmin(config)
  }

  test("Check the mini cluster for sanity") {
    assert(cluster.countServedRegions == NRegionServers, "Region Servers incorrect")
    println(s"# of region servers = ${cluster.countServedRegions}")
  }

  test("Create a test table on the server") {

    val columns = new Columns(Array.tabulate[Column](10){ ax =>
      Column(s"sqlColName$ax",s"cf${ax % 2}",s"cq${ax %2}ax",
        if (ax % 2 == 0) HBaseDataType.LONG else HBaseDataType.STRING)
    })
    val keys = new Columns(Array.tabulate[Column](4){ ax =>
      Column(s"sqlColName$ax",s"cfk${ax % 2}",s"cqk${ax %2}ax",
        if (ax % 2 == 0) HBaseDataType.LONG else HBaseDataType.STRING)
    })

    val mappingInfo = columns.columns.map{m =>
      (m.family, m.qualifier)
    }.toList

    catalog.createTable("testdb","testtaba",  columns,
      "hbasetaba", keys, mappingInfo, config)
    
    val metaTable = new HTable(config, HBaseCatalog.MetaData)
    val scanner = metaTable.getScanner(new Scan())
    import collection.mutable
    var rows = new mutable.ArrayBuffer[Result]()
    var row : Result = null
    do {
      row = scanner.next
      if (row != null) {
        rows += row
      }
    } while (row!=null)
    assert(!rows.isEmpty, "Hey where did our metadata row go?")
    val tname = rows(0).getColumnLatestCell(HBaseCatalog.ColumnFamily,
      HBaseCatalog.QualColumnInfo)
    assert(tname.getQualifierArray.contains(HBaseCatalog.QualColumnInfo),
      "We were unable to read the columnInfo cell")
  }

  override def afterAll() = {
    cluster.shutdown
    super.afterAll()
  }

}
