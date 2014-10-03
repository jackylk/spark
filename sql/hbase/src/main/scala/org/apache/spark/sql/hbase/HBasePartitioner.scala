package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.apache.spark.sql._

/**
 * HBasePartitioner
 * Created by sboesch on 10/3/14.
 */
class HBasePartitioner(hbPartitions: Array[HBasePartition])
  extends BoundedRangePartitioner(
    hbPartitions.map { part => (part.bounds.start, part.bounds.end)}) {

  override def numPartitions: Int = hbPartitions.size

  override def getPartition(key: Any): Int = {
    val row = key.asInstanceOf[Row]
    val hbaseRowKey = key.asInstanceOf[HBaseRawType]
    //      partitions.find{
    key.hashCode % numPartitions
  }
}
