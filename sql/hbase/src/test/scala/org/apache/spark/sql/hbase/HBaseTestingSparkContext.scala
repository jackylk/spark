package org.apache.spark.sql.hbase

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SharedSparkContext}
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * HBaseSharedSparkContext.  Modeled after Shared
 *
 * Created by sboesch on 9/28/14.
 */
trait HBaseTestingSparkContext extends BeforeAndAfterAll {
  self: Suite  =>
  val logger = Logger.getLogger(getClass.getName)
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  val NSlaves = 2
  val Masters = s"local[$NSlaves]"
  override def beforeAll() {
    _sc = new SparkContext(Masters, "test", conf)
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
  }
}
