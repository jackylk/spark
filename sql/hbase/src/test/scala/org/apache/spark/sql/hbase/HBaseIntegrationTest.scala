//package org.apache.spark.sql.hbase
//
//import java.sql.Timestamp
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.{Result, Scan, HTable, HBaseAdmin}
//import org.apache.log4j.Logger
//import org.apache.spark.sql.catalyst.ScalaReflection
//import org.apache.spark.sql.catalyst.types.{IntegerType, StringType, LongType}
//import org.apache.spark.sql.execution.SparkPlan
//import org.apache.spark.sql.test.TestSQLContext._
//import org.apache.spark.sql.{ReflectData, SQLContext, SchemaRDD}
////import org.apache.spark.sql.hbase.TestHbase._
//import org.apache.spark.{SparkConf, Logging, SparkContext}
//import org.apache.spark.sql.hbase.HBaseCatalog.{KeyColumn, Columns, Column}
//import org.scalatest.{Ignore, BeforeAndAfterAll, BeforeAndAfter, FunSuite}
//import org.apache.hadoop.hbase.{HBaseConfiguration, HBaseTestingUtility, MiniHBaseCluster}
//
///**
// * HBaseIntegrationTest
// * Created by sboesch on 9/27/14.
// */
//@Ignore
//class HBaseIntegrationTest extends FunSuite with BeforeAndAfterAll with Logging {
//  @transient val logger = Logger.getLogger(getClass.getName)
//
//  val NMasters = 1
//  val NRegionServers = 3
//  val NDataNodes = 0
//
//  val NWorkers = 1
//
//  var cluster : MiniHBaseCluster = _
//  var config : Configuration = _
//  var hbaseAdmin : HBaseAdmin = _
//  var hbContext : HBaseSQLContext = _
//  var catalog : HBaseCatalog = _
//  var testUtil :HBaseTestingUtility = _
//
////  @inline def assert(p: Boolean, msg: String) = {
////    if (!p) {
////      throw new IllegalStateException(s"AssertionError: $msg")
////    }
////  }
//
//  override def beforeAll() = {
//    logger.info(s"Spin up hbase minicluster w/ $NMasters mast, $NRegionServers RS, $NDataNodes dataNodes")
//    testUtil = new HBaseTestingUtility
////    cluster = HBaseTestingUtility.createLocalHTU.
////      startMiniCluster(NMasters, NRegionServers, NDataNodes)
////    config = HBaseConfiguration.create
//    config = testUtil.getConfiguration
//    config.set("hbase.regionserver.info.port","-1")
//    config.set("hbase.master.info.port","-1")
//    cluster = testUtil.startMiniCluster(NMasters, NRegionServers)
//    println(s"# of region servers = ${cluster.countServedRegions}")
//    val conf = new SparkConf
//    val SparkPort = 11223
//    conf.set("spark.ui.port",SparkPort.toString)
//    val sc = new SparkContext(s"local[$NWorkers]", "HBaseTestsSparkContext", conf)
//    hbContext = new HBaseSQLContext(sc, config)
//    catalog = hbContext.catalog
//    hbaseAdmin = new HBaseAdmin(config)
//  }
//
//  test("Check the mini cluster for sanity") {
//    assert(cluster.countServedRegions == NRegionServers, "Region Servers incorrect")
//    println(s"# of region servers = ${cluster.countServedRegions}")
//  }
//
//  val DbName = "testdb"
//  val TabName = "testtaba"
//  val HbaseTabName = "hbasetaba"
//
//  test("Create a test table on the server") {
//
////    import hbContext.
//    val columns = new Columns(Array.tabulate[KeyColumn](10){ ax =>
//      KeyColumn(s"sqlColName$ax",s"cf${ax % 2}",s"cq${ax %2}ax",
//        if (ax % 2 == 0) LongType else StringType)
//    })
//    val keys = Array.tabulate(4){ ax =>
//      KeyColumn(s"sqlColName$ax",
//        if (ax % 2 == 0) LongType else StringType)
//    }.toSeq
//
//    catalog.createTable(DbName, TabName, HbaseTabName, keys, columns)
//
//    val metaTable = new HTable(config, HBaseCatalog.MetaData)
//    val scanner = metaTable.getScanner(new Scan())
//    import collection.mutable
//    var rows = new mutable.ArrayBuffer[Result]()
//    var row : Result = null
//    do {
//      row = scanner.next
//      if (row != null) {
//        rows += row
//      }
//    } while (row!=null)
//    assert(!rows.isEmpty, "Hey where did our metadata row go?")
//    val tname = rows(0).getColumnLatestCell(HBaseCatalog.ColumnFamily,
//      HBaseCatalog.QualKeyColumns)
////    assert(new String(tname.getQualifierArray).contains(HBaseCatalog.QualColumnInfo),
////      "We were unable to read the columnInfo cell")
//    val catTab = catalog.getTable(TabName)
//    assert(catTab.get.tablename == TabName)
//    // TODO(Bo, XinYu): fix parser/Catalog to support Namespace=Dbname
//    assert(catTab.get.hbaseTableName.toString == s"$DbName:$HbaseTabName")
//  }
//
//  test("ReflectData from spark tests suite") {
//      val data = ReflectData("a", 1, 1L, 1.toFloat, 1.toDouble, 1.toShort, 1.toByte, true,
//        BigDecimal(1), new Timestamp(12345), Seq(1,2,3))
//      val rdd = sparkContext.parallelize(data :: Nil)
//      rdd.registerTempTable("reflectData")
//
//      assert(sql("SELECT * FROM reflectData").collect().head === data.productIterator.toSeq)
//
////      ctx.sql(
////        s"""insert into $TabName select * from $TempTabName""".stripMargin)
////
////      ctx.sql(s"""select * from $TabName
////    where col1 >=3 and col1 <= 10
////    order by col1 desc"""
////        .stripMargin)
//
//  }
//
//  test("get table") {
//    // prepare the test data
//    val namespace = "testNamespace"
//    val tableName = "testTable"
//    val hbaseTableName = "hbaseTable"
//
//    val oresult = catalog.getTable(tableName)
//    assert(oresult.isDefined)
//    val result = oresult.get
//    assert(result.tablename == tableName)
//    assert(result.hbaseTableName.tableName.getNameAsString == namespace + ":" + hbaseTableName)
//    assert(result.colFamilies.size === 2)
//    assert(result.columns.columns.size === 2)
//    val relation = catalog.lookupRelation(None, tableName)
//    val hbRelation = relation.asInstanceOf[HBaseRelation]
//    assert(hbRelation.colFamilies == Set("family1", "family2"))
//    assert(hbRelation.partitionKeys == Seq("column1", "column2"))
//    val rkColumns = new Columns(Seq(KeyColumn("column1",null, "column1", StringType,1),
//      KeyColumn("column1",null, "column1", IntegerType,2)))
//    assert(hbRelation.catalogTable.rowKeyColumns.equals(rkColumns))
//    assert(relation.childrenResolved)
//  }
//
//  case class MyTable(col1: String, col2: Byte, col3: Short, col4: Int, col5: Long,
//                     col6: Float, col7: Double)
//
//  test("Insert data into the test table using applySchema") {
//
//    val DbName = "mynamespace"
//    val TabName = "myTable"
//    hbContext.sql(s"""CREATE TABLE $DbName.$TabName(col1 STRING, col2 BYTE, col3 SHORT, col4 INTEGER,
//      col5 LONG, col6 FLOAT, col7 DOUBLE)
//      MAPPED BY (hbaseTableName, KEYS=[col7, col1, col3], COLS=[col2=cf1.cq11,
//      col4=cf1.cq12, col5=cf2.cq21, col6=cf2.cq22])"""
//      .stripMargin)
//
//    val catTab = catalog.getTable(TabName)
//    assert(catTab.get.tablename == TabName)
//
//    val ctx = hbContext
//    import ctx.createSchemaRDD
//    val myRows = ctx.sparkContext.parallelize(Range(1,21).map{ix =>
//      MyTable(s"col1$ix", ix.toByte, (ix.toByte*256).asInstanceOf[Short],ix.toByte*65536, ix.toByte*65563L*65536L,
//        (ix.toByte*65536.0).asInstanceOf[Float], ix.toByte*65536.0D*65563.0D)
//    })
//
////    import org.apache.spark.sql.execution.ExistingRdd
////    val myRowsSchema = ExistingRdd.productToRowRdd(myRows)
////    ctx.applySchema(myRowsSchema, schema)
//    val TempTabName = "MyTempTab"
//    myRows.registerTempTable(TempTabName)
//
//    //    ctx.sql(
//    //      s"""insert into $TabName select * from $TempTabName""".stripMargin)
//
//    val hbRelation = catalog.lookupRelation(Some(DbName), TabName).asInstanceOf[HBaseRelation]
//
//    val hbasePlanner = new SparkPlanner with HBaseStrategies {
//      @transient override val hbaseContext: HBaseSQLContext = hbContext
//    }
//
//    val myRowsSchemaRdd = hbContext.createSchemaRDD(myRows)
//    val insertPlan = hbasePlanner.InsertIntoHBaseTableFromRdd(hbRelation,
//        myRowsSchemaRdd)(hbContext)
//
//    val insertRdd = insertPlan.execute.collect
//
//    ctx.sql( s"""select * from $TabName
//    where col1 >=3 and col1 <= 10
//    order by col1 desc"""
//      .stripMargin)
//
//  }
//
//  test("Run a simple query") {
//    // ensure the catalog exists (created in the "Create a test table" test)
//    val catTab = catalog.getTable(TabName).get
//    assert(catTab.tablename == TabName)
//    val rdd = hbContext.sql(s"select * from $TabName")
//    rdd.take(1)
//
//  }
//
//    override def afterAll() = {
//    cluster.shutdown
//  }
//
//}
