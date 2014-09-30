package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan, HTable, HBaseAdmin}
import org.apache.log4j.Logger
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.hbase.HBaseCatalog.{KeyColumn, Columns, HBaseDataType, Column}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}
import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, MiniHBaseCluster}

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
class HBaseIntegrationTest extends FunSuite with BeforeAndAfterAll with Logging {
  val logger = Logger.getLogger(getClass.getName)

  val NMasters = 1
  val NRegionServers = 3
  val NDataNodes = 0

  val NWorkers = 1

  var hbContext : HBaseSQLContext = _
  var cluster : MiniHBaseCluster = _
  var config : Configuration = _
  var hbaseAdmin : HBaseAdmin = _
  var catalog : HBaseCatalog = _
  var testUtil :HBaseTestingUtility = _

  override def beforeAll() = {
    logger.info(s"Spin up hbase minicluster w/ $NMasters mast, $NRegionServers RS, $NDataNodes dataNodes")
    testUtil = new HBaseTestingUtility
//    cluster = HBaseTestingUtility.createLocalHTU.
//      startMiniCluster(NMasters, NRegionServers, NDataNodes)
//    config = HBaseConfiguration.create
    config = testUtil.getConfiguration
    config.set("hbase.regionserver.info.port","-1")
    config.set("hbase.master.info.port","-1")
    cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
    println(s"# of region servers = ${cluster.countServedRegions}")
    val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext")
    hbContext = new HBaseSQLContext(sc, config)
    catalog = hbContext.catalog
    hbaseAdmin = new HBaseAdmin(config)
  }

  test("Check the mini cluster for sanity") {
    assert(cluster.countServedRegions == NRegionServers, "Region Servers incorrect")
    println(s"# of region servers = ${cluster.countServedRegions}")
  }

  val DbName = "testdb"
  val TabName = "testtaba"
  val HbaseTabName = "hbasetaba"

  test("Create a test table on the server") {

    val columns = new Columns(Array.tabulate[Column](10){ ax =>
      Column(s"sqlColName$ax",s"cf${ax % 2}",s"cq${ax %2}ax",
        if (ax % 2 == 0) HBaseDataType.LONG else HBaseDataType.STRING)
    })
    val keys = Array.tabulate(4){ ax =>
      KeyColumn(s"sqlColName$ax",
        if (ax % 2 == 0) HBaseDataType.LONG else HBaseDataType.STRING)
    }.toSeq

    catalog.createTable(DbName, TabName, HbaseTabName, keys, columns)

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
      HBaseCatalog.QualKeyColumns)
//    assert(new String(tname.getQualifierArray).contains(HBaseCatalog.QualColumnInfo),
//      "We were unable to read the columnInfo cell")
    val catTab = catalog.getTable(DbName, TabName)
    assert(catTab.tablename == TabName)
    // TODO(Bo, XinYu): fix parser/Catalog to support Namespace=Dbname
    assert(catTab.hbaseTableName.toString == s"$DbName:$HbaseTabName")
  }

  test("Run a simple query") {
    // ensure the catalog exists (created in the "Create a test table" test)
    val catTab = catalog.getTable(DbName, TabName)
    assert(catTab.tablename == TabName)
    val rdd = hbContext.sql(s"select * from $TabName")
    rdd.take(1)

  }

    override def afterAll() = {
    cluster.shutdown
    hbContext.stop
  }

}
