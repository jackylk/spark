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
import org.apache.spark.Partition
import org.apache.spark.sql.hbase._

/**
 * HBasePartition
 * Created by sboesch on 9/9/14.
 */
case class HBasePartitionBounds(start : Option[HBaseRawType], end: Option[HBaseRawType]) {

  def contains(rowKey: Optionable[HBaseRawType]) = {
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
    !rowKey.opt.isEmpty && cmp(rowKey.opt, start) >= 0 && cmp(rowKey.opt, end) <= 0
  }
}

case class HBasePartition(idx : Int, bounds : HBasePartitionBounds,
                          server: Option[String])  extends Partition {

  /**
   * Get the split's index within its parent RDD
   */
  override def index: Int = idx

}
object HBasePartition {
  val SinglePartition = new HBasePartition(1, HBasePartitionBounds(None, None), None)
}
