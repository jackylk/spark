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
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger

/**
 * HBaseUtils
 * This class needs to be serialized to the Spark Workers so let us keep it slim/trim
 *
 * Created by sboesch on 9/16/14.
 */
object HBaseUtils extends Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  @transient private lazy val lazyConfig = HBaseConfiguration.create()

  def configuration() = lazyConfig

  def getHBaseConnection(configuration: Configuration) = {
    val connection = HConnectionManager.createConnection(configuration)
    connection
  }

  def getPartitions(tableName: TableName,
                    config: Configuration) = {
    import scala.collection.JavaConverters._
    val hConnection = getHBaseConnection(config)
    val regionLocations = hConnection.locateRegions(tableName)
    case class BoundsAndServers(startKey: HBaseRawType, endKey: HBaseRawType,
                                servers: Seq[String])
    val regionBoundsAndServers = regionLocations.asScala.map { hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      BoundsAndServers(regionInfo.getStartKey, regionInfo.getEndKey,
        Seq(hregionLocation.getServerName.getHostname))
    }
    val partSeq = regionBoundsAndServers.zipWithIndex.map { case (rb, ix) =>
      new HBasePartition(ix, HBasePartitionBounds(Some(rb.startKey), Some(rb.endKey)),
        Some(rb.servers(0)))
    }
    partSeq.toIndexedSeq
  }


}
