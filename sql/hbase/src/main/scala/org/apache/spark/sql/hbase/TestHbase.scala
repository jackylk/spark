package org.apache.spark.sql.hbase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLConf, SQLContext}

/** A SQLContext that can be used for local testing. */
object TestHbase
  extends HBaseSQLContext(new SparkContext("local[2]", "TestSQLContext", new SparkConf())) {

  /** Fewer partitions to speed up testing. */
  override private[spark] def numShufflePartitions: Int =
    getConf(SQLConf.SHUFFLE_PARTITIONS, "5").toInt
}