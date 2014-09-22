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
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}
import org.apache.log4j.Logger

import scala.collection.JavaConverters

/**
 * HBaseUtils
 * Created by sboesch on 9/16/14.
 */
object HBaseUtils {
  val logger = Logger.getLogger(getClass.getName)

  def getConfiguration(hbaseContext : HBaseSQLContext)  =
  hbaseContext.sparkContext.getConf.get("hadoop.configuration")
    .asInstanceOf[Configuration]

  def getHBaseConnection(configuration : Configuration)  = {
    val connection = HConnectionManager.createConnection(configuration)
    connection
  }

  def getPartitions(hConnection : HConnection, tableName : String) = {
    import JavaConverters._
    val regionLocations = hConnection.locateRegions(TableName.valueOf(tableName))
    case class BoundsAndServers(startKey : Array[Byte], endKey : Array[Byte],
                                servers : Seq[String])
    val regionBoundsAndServers = regionLocations.asScala.map{ hregionLocation =>
      val regionInfo = hregionLocation.getRegionInfo
      BoundsAndServers( regionInfo.getStartKey, regionInfo.getEndKey,
        Seq(hregionLocation.getServerName.getHostname))
    }
    regionBoundsAndServers.zipWithIndex.map{ case (rb,ix) =>
      new HBasePartition(ix, (rb.startKey, rb.endKey), rb.servers(0))
    }
  }

}
