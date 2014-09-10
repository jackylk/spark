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
import org.apache.spark.Partitioner

/**
 * BoundedRangePartitioner
 * Created by sboesch on 9/9/14.
 */
//  class BoundedRangePartitioner( bounds: Seq[(Array[Byte],Array[Byte])]) extends Partitioner {
class BoundedRangePartitioner[K <: Comparable[K] ] ( bounds: Seq[(K,K)]) extends Partitioner {
  override def numPartitions: Int = bounds.size

  override def getPartition(key: Any): Int = {
    val keyComp = key.asInstanceOf[Comparable[K]]
    var partNum = bounds.size / 2
    var incr = bounds.size / 4
    var found = false
    do {
      if (keyComp.compareTo(bounds(partNum)._1) <0) {
        partNum -= incr
      } else if (keyComp.compareTo(bounds(partNum)._2) > 0) {
        partNum += incr
      } else {
        found = true
      }
      incr /= 2
    } while (!found && incr > 0)
    if (!found) {
      throw new IllegalArgumentException
      (s"Unable to locate key $key within HBase Region boundaries")
    }
    partNum
  }
}
