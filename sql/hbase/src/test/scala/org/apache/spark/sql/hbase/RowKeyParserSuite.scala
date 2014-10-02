package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.scalatest.{ShouldMatchers, FunSuite}
import HBaseUtils._

/**
 * CompositeRowKeyParserTest
 * Created by sboesch on 9/25/14.
 */
class RowKeyParserSuite extends FunSuite with ShouldMatchers {
  val logger = Logger.getLogger(getClass.getName)

  test("rowkey test") {
    val cols = Range(0, 4).map { ix =>
      ColumnName(s"cf${ix + 1}", s"cq${ix + 10}")
    }.toSeq

    val pat = "Hello1234GoHome".getBytes("ISO-8859-1")
    val parsedKeyMap = RowKeyParser.parseRowKeyWithMetaData(cols, pat)
    println(s"parsedKeyWithMetaData: ${parsedKeyMap.toString}")

    val parsedKey = RowKeyParser.parseRowKey(pat)
    println(s"parsedKeyWithMetaData: ${parsedKey.toString}")

  }

}
