package org.apache.spark.sql.hbase

import java.io.{ObjectOutputStream, ByteArrayOutputStream, DataOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.{DoubleType, ShortType, StringType}
import org.apache.spark.sql.hbase.DataTypeUtils._
import org.apache.spark.sql.hbase.HBaseCatalog.{Column, Columns}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.{Logging, SparkConf, sql}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.sql.Row

/**
 * HBaseIntegrationTest
 * Created by sboesch on 9/27/14.
 */
object HBaseMainTest extends FunSuite with BeforeAndAfterAll with Logging {
  @transient val logger = Logger.getLogger(getClass.getName)

  val useMiniCluster: Boolean = false

  val NMasters = 1
  val NRegionServers = 1
  // 3
  val NDataNodes = 0

  val NWorkers = 1

  @transient var cluster: MiniHBaseCluster = null
  @transient var config: Configuration = null
  @transient var hbaseAdmin: HBaseAdmin = null
  @transient var hbContext: HBaseSQLContext = null
  @transient var catalog: HBaseCatalog = null
  @transient var testUtil: HBaseTestingUtility = null

  case class MyTable(col1: String, col2: Byte, col3: Short, col4: Int, col5: Long,
                     col6: Float, col7: Double)

  val DbName = "mynamespace"
  val TabName = "myTable"
  val HbaseTabName = "hbasetaba"

  def ctxSetup() {
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
    config.set("hbase.regionserver.lease.period", "240000")

    if (useMiniCluster) {
      cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
      println(s"# of region servers = ${cluster.countServedRegions}")
    }

    @transient val conf = new SparkConf
    val SparkPort = 11223
    conf.set("spark.ui.port", SparkPort.toString)
    //    @transient val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext", conf)
    hbContext = new HBaseSQLContext(TestSQLContext.sparkContext, config)

    catalog = hbContext.catalog
    hbaseAdmin = new HBaseAdmin(config)

  }

  def tableSetup() = {
    createTable()
  }

  def createTable() = {

    val createTable = useMiniCluster
    if (createTable) {
      try {
        hbContext.sql( s"""CREATE TABLE $TabName(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
          col5 LONG, col6 FLOAT, col7 DOUBLE)
          MAPPED BY ($HbaseTabName, KEYS=[col7, col1, col3], COLS=[col2=cf1.cq11,
          col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
          .stripMargin)
      } catch {
        case e: TableExistsException =>
          e.printStackTrace
      }

      try {
        val hdesc = new HTableDescriptor(TableName.valueOf(HbaseTabName))
        Array(new HColumnDescriptor("cf1"), new HColumnDescriptor("cf2")).foreach { f =>
          hdesc.addFamily(f)
        }
        hbaseAdmin.createTable(hdesc)
      } catch {
        case e: TableExistsException =>
          e.printStackTrace
      }
    }

    if (!hbaseAdmin.tableExists(HbaseTabName)) {
      throw new IllegalArgumentException("where is our table?")
    }

  }

  def testGetTable = {
    println("get table")
    // prepare the test data
    HBaseCatalog.getKeysFromAllMetaTableRows(config)
      .foreach { r => logger.info(s"Metatable Rowkey: ${new String(r)}")}

    val oresult = catalog.getTable(TabName)
    assert(oresult.isDefined)
    val result = oresult.get
    assert(result.tablename == TabName)
    assert(result.hbaseTableName.tableName.getNameAsString == DbName + ":" + HbaseTabName)
    assert(result.colFamilies.size == 2)
    assert(result.columns.columns.size == 4)
    assert(result.rowKeyColumns.columns.size == 3)
    val relation = catalog.lookupRelation(Some(DbName), TabName)
    val hbRelation = relation.asInstanceOf[HBaseRelation]
    assert(hbRelation.colFamilies == Seq("cf1", "cf2"))
    assert(Seq("col7", "col1", "col3").zip(hbRelation.partitionKeys)
      .forall { x => x._1 == x._2.name})
    val rkColumns = new Columns(Seq(Column("col7", null, "col7", DoubleType),
      Column("col1", null, "col1", StringType),
      Column("col3", null, "col3", ShortType)))
    assert(hbRelation.catalogTable.rowKeyColumns.equals(rkColumns))
    assert(relation.childrenResolved)
  }

  def checkHBaseTableExists(hbaseTable: String) = {
    hbaseAdmin.listTableNames.foreach { t => println(s"table: $t")}
    val tname = TableName.valueOf(hbaseTable)
    hbaseAdmin.tableExists(tname)
  }

  def insertTestData() = {
    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table ${HbaseTabName}")
    }
    val htable = new HTable(config, HbaseTabName)

    var put = new Put(makeRowKey(12345.0, "Michigan", 12345))
    addRowVals(put, (123).toByte, 12345678, 12345678901234L, 1234.5678F)
    htable.put(put)
    put = new Put(makeRowKey(456789.0, "Michigan", 4567))
    addRowVals(put, (456).toByte, 456789012, 4567890123446789L, 456.78901F)
    htable.put(put)
    htable.close

  }

  val runMultiTests: Boolean = false

  def testQuery() {
    ctxSetup()
    createTable()
    //    testInsertIntoTable
    //    testHBaseScanner

    if (!checkHBaseTableExists(HbaseTabName)) {
      throw new IllegalStateException(s"Unable to find table ${HbaseTabName}")
    }

    insertTestData

    var results: SchemaRDD = null
    var data: Array[sql.Row] = null

      results = hbContext.sql( s"""SELECT * FROM $TabName """.stripMargin)
      printResults("Star* operator", results)
      data = results.collect
      assert(data.size >= 2)

      results = hbContext.sql(
        s"""SELECT col3, col1, col7 FROM $TabName LIMIT 1
             """.stripMargin)
      printResults("Limit Op", results)
      data = results.collect
      assert(data.size == 1)

      results = hbContext.sql(
        s"""SELECT col3, col2, col1, col4, col7 FROM $TabName order by col7 desc
             """.stripMargin)
      printResults("Ordering with nonkey columns", results)
      data = results.collect
      assert(data.size >= 2)

      try {
        results = hbContext.sql(
          s"""SELECT col3, col1, col7 FROM $TabName LIMIT 1
             """.stripMargin)
        printResults("Limit Op", results)
      } catch {
        case e: Exception => "Query with Limit failed"
          e.printStackTrace
      }

      results = hbContext.sql( s"""SELECT col3, col1, col7 FROM $TabName ORDER  by col7 DESC
      """.stripMargin)
      printResults("Order by", results)

    if (runMultiTests) {
      results = hbContext.sql( s"""SELECT col3, col2, col1, col7, col4 FROM $TabName
          WHERE col1 ='Michigan'
          """.stripMargin)
      printResults("Where/filter on rowkey", results)
      data = results.collect
      assert(data.size >= 1)

      results = hbContext.sql( s"""SELECT col7, col3, col2, col1, col4 FROM $TabName
          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 3500 and col3 <= 5000
          """.stripMargin)
      printResults("Where/filter on rowkeys change", results)

      results = hbContext.sql( s"""SELECT col3, col2, col1, col7, col4 FROM $TabName
          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 3500 and col3 <= 5000
          """.stripMargin)
      printResults("Where/filter on rowkeys", results)


      results = hbContext.sql( s"""SELECT col1, col3, col7 FROM $TabName
          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col3 != 7.0
          """.stripMargin)
      printResults("Where with notequal", results)

      results = hbContext.sql( s"""SELECT col1, col2, col3, col7 FROM $TabName
          WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and cast(col2 as double) != 7.0
          """.stripMargin)
      printResults("Include non-rowkey cols in project", results)
    }
    if (runMultiTests) {
      results = hbContext.sql( s"""SELECT col1, col2, col3, col7 FROM $TabName
        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col2 != 7.0
        """.stripMargin)
      printResults("Include non-rowkey cols in filter", results)

      results = hbContext.sql( s"""SELECT sum(col3) as col3sum, col1, col3 FROM $TabName
        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 and col2 != 7.0
        group by col1,  col3
        """.stripMargin)
      printResults("Aggregates on rowkeys", results)


      results = hbContext.sql( s"""SELECT sum(col2) as col2sum, col4, col1, col3, col2 FROM $TabName
        WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50
        group by col1, col2, col4, col3
        """.stripMargin)
      printResults("Aggregates on non-rowkeys", results)
    }
  }

  def printResults(msg: String, results: SchemaRDD) = {
    if (results.isInstanceOf[TestingSchemaRDD]) {
      val data = results.asInstanceOf[TestingSchemaRDD].collectPartitions
      println(s"For test [$msg]: Received data length=${data(0).length}: ${
        data(0).mkString("RDD results: {", "],[", "}")
      }")
    } else {
      val data = results.collect
      println(s"For test [$msg]: Received data length=${data.length}: ${
        data.mkString("RDD results: {", "],[", "}")
      }")
    }

  }

  def createTableTest2() {
    ctxSetup()
    // Following fails with Unresolved:
    // Col1 Sort is unresolved
    // Col4 and col2 Aggregation are unresolved (interesting col3 IS resolved)
    //    val results = hbContext.sql(s"""SELECT col4, col1, col3, col2 FROM $TabName
    //    WHERE col1 ='Michigan' and col7 >= 2500.0 and col3 >= 35 and col3 <= 50 group by col7, col1
    //    ORDER BY col1 DESC"""
    //      .stripMargin)

    hbContext.sql( s"""CREATE TABLE $DbName.$TabName(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
      col5 LONG, col6 FLOAT, col7 DOUBLE)
      MAPPED BY ($HbaseTabName, KEYS=[col7, col1, col3], COLS=[col2=cf1.cq11,
      col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
      .stripMargin)

    val catTab = catalog.getTable(TabName)
    assert(catTab.get.tablename == TabName)

    testGetTable
  }

  def testInsertIntoTable() = {
    logger.info("Insert data into the test table using applySchema")
    ctxSetup()
    tableSetup()
    //    import hbContext.createSchemaRDD
    val myRows = hbContext.sparkContext.parallelize(Range(1, 21).map { ix =>
      MyTable(s"Michigan", ix.toByte, (ix.toByte * 256).asInstanceOf[Short], ix.toByte * 65536, ix.toByte * 65563L * 65536L,
        (ix.toByte * 65536.0).asInstanceOf[Float], ix.toByte * 65536.0D * 65563.0D)
    })

    //    import org.apache.spark.sql.execution.ExistingRdd
    //    val myRowsSchema = ExistingRdd.productToRowRdd(myRows)
    //    hbContext.applySchema(myRowsSchema, schema)
    val TempTabName = "MyTempTab"
    myRows.registerTempTable(TempTabName)

    val localData = myRows.collect

    hbContext.sql(
      s"""insert into $TabName select * from $TempTabName""".stripMargin)

    val hbRelation = catalog.lookupRelation(Some(DbName), TabName).asInstanceOf[HBaseRelation]

    val hbasePlanner = new SparkPlanner with HBaseStrategies {
      @transient override val hbaseContext: HBaseSQLContext = hbContext
    }

    val myRowsSchemaRdd = hbContext.createSchemaRDD(myRows)
    val insertPlan = hbasePlanner.InsertIntoHBaseTableFromRdd(hbRelation,
      myRowsSchemaRdd)(hbContext)

    var rowKeysWithRows = myRowsSchemaRdd.zip(
      HBaseRelation.rowKeysFromRows(myRowsSchemaRdd, hbRelation))
    //    var keysCollect = rowKeysWithRows.collect
    HBaseStrategies.putToHBase(myRows, hbRelation, hbContext)

    val preparedInsertRdd = insertPlan.execute
    val executedInsertRdd = preparedInsertRdd.collect

    val rowsRdd = myRowsSchemaRdd
    val rowKeysWithRows2 = rowsRdd.zip(
      HBaseRelation.rowKeysFromRows(rowsRdd, hbRelation))
    HBaseStrategies.putToHBase(rowsRdd, hbRelation, hbContext)


    cluster.shutdown
  }

  import org.apache.spark.sql.hbase.RowKeyParser._

  def makeRowKey(col7: Double, col1: String, col3: Short) = {
    val size = 1 + sizeOf(col7) + sizeOf(col1) + sizeOf(col3) + 3 * 2 + DimensionCountLen
    //      val barr = new Array[Byte](size)
    val bos = new ByteArrayOutputStream(size)
    val dos = new DataOutputStream(bos)
    dos.writeByte(RowKeyParser.Version1)
    dos.writeDouble(col7)
    dos.writeBytes(col1)
    dos.writeShort(col3)
    var off = 1
    dos.writeShort(off)
    off += sizeOf(col7)
    dos.writeShort(off)
    off += sizeOf(col1)
    dos.writeShort(off)
    dos.writeByte(3.toByte)
    val s = bos.toString
    //      println((s"MakeRowKey: [${RowKeyParser.show(bos.toByteArray)}]")
    println(s"MakeRowKey: [${s}]")
    bos.toByteArray
  }

  def addRowVals(put: Put, col2: Byte, col4: Int, col5: Long, col6: Float) = {
    //      val barr = new Array[Byte](size)
    var bos = new ByteArrayOutputStream()
    var dos = new DataOutputStream(bos)
    dos.writeByte(col2)
    put.add(s2b("cf1"), s2b("cq11"), bos.toByteArray)
    bos = new ByteArrayOutputStream()
    dos = new DataOutputStream(bos)
    dos.writeInt(col4)
    put.add(s2b("cf1"), s2b("cq12"), bos.toByteArray)
    bos = new ByteArrayOutputStream()
    dos = new DataOutputStream(bos)
    dos.writeLong(col5)
    put.add(s2b("cf2"), s2b("cq21"), bos.toByteArray)
    bos = new ByteArrayOutputStream()
    dos = new DataOutputStream(bos)
    dos.writeFloat(col6)
    put.add(s2b("cf2"), s2b("cq22"), bos.toByteArray)
  }

  def testHBaseScanner() = {
    val scan = new Scan
    val htable = new HTable(config, HbaseTabName)
    val scanner = htable.getScanner(scan)
    var res: Result = null
    do {
      res = scanner.next
      if (res != null) println(s"Row ${res.getRow} has map=${res.getNoVersionMap.toString}")
    } while (res != null)
  }

  def main(args: Array[String]) = {
    //    testInsertIntoTable
    testQuery
  }

}
