package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseTestingUtility, MiniHBaseCluster}
import org.apache.log4j.Logger
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
object HBaseMainTest extends FunSuite with BeforeAndAfterAll with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val NMasters = 1
  val NRegionServers = 3
  val NDataNodes = 0

  val NWorkers = 1


  @inline def assert(p: Boolean, msg: String) = {
    if (!p) {
      throw new IllegalStateException(s"AssertionError: $msg")
    }
  }

  case class MyTable(col1: String, col2: Byte, col3: Short, col4: Int, col5: Long,
                     col6: Float, col7: Double)

  def main(args: Array[String]) = {
logger.info("Insert data into the test table using applySchema")
    @transient var cluster : MiniHBaseCluster = null
    @transient var config : Configuration = null
    @transient var hbaseAdmin : HBaseAdmin = null
    @transient var hbContext : HBaseSQLContext = null
    @transient var catalog : HBaseCatalog = null
    @transient var testUtil :HBaseTestingUtility = null

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
    @transient val conf = new SparkConf
    val SparkPort = 11223
    conf.set("spark.ui.port",SparkPort.toString)
    @transient val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext", conf)
    hbContext = new HBaseSQLContext(sc, config)
    import java.io._
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(hbContext)
    println(new String(bos.toByteArray))

    catalog = hbContext.catalog
    hbaseAdmin = new HBaseAdmin(config)


    val DbName = "mynamespace"
    val TabName = "myTable"
    val HbaseTabName = "hbasetaba"

    hbContext.sql(s"""CREATE TABLE $DbName.$TabName(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
      col5 LONG, col6 FLOAT, col7 DOUBLE)
      MAPPED BY (hbaseTableName, KEYS=[col7, col1, col3], COLS=[col2=cf1.cq11,
      col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
      .stripMargin)

    val catTab = catalog.getTable(Some(DbName), TabName)
    assert(catTab.get.tablename == TabName)

    val ctx = hbContext

    val results = ctx.sql(s"""SELECT col4, col1, col3, col2 FROM $DbName.$TabName
    WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 group by col7, col1
    ORDER BY col1 DESC"""
      .stripMargin)

//    val results = ctx.sql(s"""SELECT * FROM $DbName.$TabName
//    WHERE col1 >=3 AND col1 <= 10
//    ORDER BY col1 DESC"""
//      .stripMargin)

    val data = results.collect


    import ctx.createSchemaRDD
    val myRows = ctx.sparkContext.parallelize(Range(1,21).map{ix =>
      MyTable(s"col1$ix", ix.toByte, (ix.toByte*256).asInstanceOf[Short],ix.toByte*65536, ix.toByte*65563L*65536L,
        (ix.toByte*65536.0).asInstanceOf[Float], ix.toByte*65536.0D*65563.0D)
    })

    //    import org.apache.spark.sql.execution.ExistingRdd
    //    val myRowsSchema = ExistingRdd.productToRowRdd(myRows)
    //    ctx.applySchema(myRowsSchema, schema)
    val TempTabName = "MyTempTab"
    myRows.registerTempTable(TempTabName)

    val localData = myRows.collect

    //    ctx.sql(
    //      s"""insert into $TabName select * from $TempTabName""".stripMargin)

    val hbRelation = catalog.lookupRelation(Some(DbName), TabName).asInstanceOf[HBaseRelation]


    val hbasePlanner = new SparkPlanner with HBaseStrategies {
      @transient override val hbaseContext: HBaseSQLContext = hbContext
    }

    val myRowsSchemaRdd = hbContext.createSchemaRDD(myRows)
    val insertPlan = hbasePlanner.InsertIntoHBaseTableFromRdd(hbRelation,
      myRowsSchemaRdd)(hbContext)

    var rowKeysWithRows = myRowsSchemaRdd.zip(
      HBaseStrategies.rowKeysFromRows(myRowsSchemaRdd,hbRelation))
//    var keysCollect = rowKeysWithRows.collect
    HBaseStrategies.putToHBaseLocal(myRows.schema, hbRelation, hbContext, rowKeysWithRows)


    val preparedInsertRdd = insertPlan.execute
    val executedInsertRdd = preparedInsertRdd.collect

    val rowsRdd = myRowsSchemaRdd
    val rowKeysWithRows2 = rowsRdd.zip(
      HBaseStrategies.rowKeysFromRows(rowsRdd,hbRelation))
    HBaseStrategies.putToHBaseLocal(rowsRdd.schema, hbRelation, hbContext, rowKeysWithRows2)


    cluster.shutdown
    hbContext.stop
  }
}