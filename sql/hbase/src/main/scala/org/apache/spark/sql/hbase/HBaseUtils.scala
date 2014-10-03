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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

import scala.collection.JavaConverters

/**
 * HBaseUtils
 * This class needs to be serialized to the Spark Workers so let us keep it slim/trim
 *
 * Created by sboesch on 9/16/14.
 */
object HBaseUtils extends Serializable {
  @transient val logger = Logger.getLogger(getClass.getName)

  @transient private lazy val lazyConfig =   HBaseConfiguration.create()
  def configuration() = lazyConfig

  def getHBaseConnection(configuration : Configuration)  = {
    val connection = HConnectionManager.createConnection(configuration)
    connection
  }

  def getPartitions(tableName : TableName,
                    config : Configuration) = {
    import scala.collection.JavaConverters._
    val hConnection = getHBaseConnection(config)
    val regionLocations = hConnection.locateRegions(tableName)
    case class BoundsAndServers(startKey : Array[Byte], endKey : Array[Byte],
                                servers : Seq[String])
    val regionBoundsAndServers = regionLocations.asScala.map{ hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      BoundsAndServers( regionInfo.getStartKey, regionInfo.getEndKey,
        Seq(hregionLocation.getServerName.getHostname))
    }
    val partSeq = regionBoundsAndServers.zipWithIndex.map{ case (rb,ix) =>
      new HBasePartition(ix, HBasePartitionBounds(Some(rb.startKey), Some(rb.endKey)),
        Some(rb.servers(0)))
    }
    partSeq.toIndexedSeq
  }

  def cmp(str1: Option[HBaseRawType], str2: Option[HBaseRawType]) = {
    if (str1.isEmpty && str2.isEmpty) 0
    else if (str1.isEmpty) -2
    else if (str2.isEmpty) 2
    else {
      var ix = 0
      val s1arr = str1.get
      val s2arr = str2.get
      var retval : Option[Int] = None
      while (ix >= str1.size && ix >= str2.size && retval.isEmpty) {
        if (s1arr(ix) != s2arr(ix)) {
          retval = Some(Math.signum(s1arr(ix) - s2arr(ix)).toInt)
        }
      }
      retval.getOrElse(
        if (s1arr.length == s2arr.length) {
          0
        } else {
          Math.signum(s1arr.length - s2arr.length).toInt
        }
      )
    }
  }

}
