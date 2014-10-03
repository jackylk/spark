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

import org.apache.log4j.Logger
import org.apache.spark.{Logging, Partitioner}
import org.apache.spark.sql._

/**
 * HBasePartitioner
 * Created by sboesch on 10/3/14.
 */
class HBasePartitioner(hbPartitions: Array[HBasePartition]) extends Partitioner with Logging {
  //  extends BoundedRangePartitioner(
  //    hbPartitions.map { part => (part.bounds.start.getOrElse(MinByteArr),
  //      part.bounds.end.getOrElse(MaxByteArr))
  //    }) {

  type RowKeyType = HBaseRawType
  val DefaultPartitionIfNotFound = 0

  val bounds = hbPartitions.map { part => (part.bounds.start.getOrElse(MinByteArr),
    part.bounds.end.getOrElse(MaxByteArr))
  }

  override def numPartitions: Int = hbPartitions.size

  override def getPartition(key: Any): Int = {
    //    val keyComp = key.asInstanceOf[Comparable[K]]
    val rkey = key.asInstanceOf[RowKeyType]
    var found = false
    // TODO(sboesch): ensure the lower bounds = Lowest possible value
    // and upper bounds = highest possible value for datatype.
    // If empty then coerce to these values

    import collection.mutable
    val lowerBounds = bounds.map {
      _._1
    }.foldLeft(mutable.ArrayBuffer[RowKeyType]()) { case (arr, b) =>
      arr += b
      arr
    }.asInstanceOf[IndexedSeq[RowKeyType]]

    val lowerBound = binarySearchLowerBound[RowKeyType, RowKeyType](lowerBounds, rkey,
        { key => key}).getOrElse {
      val keyval = rkey match {
        case arr: Array[Byte] => new String(arr)
        case x => x.toString
      }
      logError(s"Unable to find correct partition for key [$keyval] " +
        s"so using partition $DefaultPartitionIfNotFound")
      DefaultPartitionIfNotFound
    }
    val partIndex = bounds.map {
      _._1
    }.indexOf(lowerBound)
    partIndex
  }


}
