/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.log4j.Logger
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{Dependency, Partition}

/**
 * HBaseSQLRDD
 * Created by sboesch on 9/15/14.
 */
@AlphaComponent
abstract class HBaseSQLRDD(
                            tableName: TableName,
                            externalResource: Option[HBaseExternalResource],
                            partitions: Seq[HBasePartition],
                            @transient hbaseContext: HBaseSQLContext)
  extends RDD[Row](hbaseContext.sparkContext, Nil) {

  val logger = Logger.getLogger(getClass.getName)

  // The SerializedContext will contain the necessary instructions
  // for all Workers to know how to connect to HBase
  // For now just hardcode the Config/connection logic
  @transient lazy val configuration = HBaseUtils.configuration
  @transient lazy val connection = HBaseUtils.getHBaseConnection(configuration)

  lazy val hbPartitions = HBaseUtils.
    getPartitions(tableName,
      hbaseContext.configuration). /* unzip._1 . */ toArray[Partition]

  override def getPartitions: Array[Partition] = hbPartitions

  // TODO(sboesch): getting error: method partitioner needs to be stable, immutable value
//  override def partitioner = Some(new Partitioner() {
//    override def numPartitions: Int = hbPartitions.size
//
//    override def getPartition(key: Any): Int = {
//      // TODO(sboesch):  How is the "key" determined for a SchemaRDD Row object??
//      // the documentation for the more general RDD (not SchemaRDD..) says it is
//      // based on the grouping/aggregation "key" for groupBy/cogroup/aggregate.
//      // But that key is not useful for us!  Need to look more into this..
//      val hbaseRowKey = key.asInstanceOf[HBaseRawType]
//      //      partitions.find{
//      key.hashCode % numPartitions
//    }
//  })
  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map{identity}.toSeq
  }
}
