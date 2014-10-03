package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.hbase.HBaseCatalog.{Column, Columns, HBaseDataType, KeyColumn}
import org.apache.spark.{Logging, SparkContext, _}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by mengbo on 10/2/14.
 */
class CatalogTest extends FunSuite with BeforeAndAfterAll with Logging {
  var sparkConf: SparkConf = _
  var sparkContext: SparkContext = _
  var hbaseContext: HBaseSQLContext = _
  var configuration: Configuration = _
  var catalog: HBaseCatalog = _

  override def beforeAll() = {
    sparkConf = new SparkConf().setAppName("Catalog Test").setMaster("local[4]")
    sparkContext = new SparkContext(sparkConf)
    hbaseContext = new HBaseSQLContext(sparkContext)
    configuration = HBaseConfiguration.create()
    catalog = new HBaseCatalog(hbaseContext, configuration)
  }

  test("create table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val keyColumn1 = KeyColumn("column1", HBaseDataType.STRING)
    val keyColumn2 = KeyColumn("column2", HBaseDataType.INTEGER)
    var keyColumns = List[KeyColumn]()
    keyColumns = keyColumns :+ keyColumn1
    keyColumns = keyColumns :+ keyColumn2

    val nonKeyColumn3 = Column("column3", "family1", "qualifier1", HBaseDataType.BOOLEAN)
    val nonKeyColumn4 = Column("column4", "family2", "qualifier2", HBaseDataType.FLOAT)
    var nonKeyColumnList = List[Column]()
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn3
    nonKeyColumnList = nonKeyColumnList :+ nonKeyColumn4
    val nonKeyColumns = new Columns(nonKeyColumnList)

    catalog.createTable(namespace, tableName, hbaseTableName, keyColumns, nonKeyColumns)
  }

  test("get table") {
    // prepare the test data
    val namespace = "testNamespace"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val result = catalog.getTable(namespace, tableName)
    assert(result.tablename === tableName)
    assert(result.hbaseTableName.getNameAsString === namespace + ":" + hbaseTableName)
    assert(result.colFamilies.size === 2)
    assert(result.columns.columns.size === 2)
  }
}
