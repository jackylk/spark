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

import org.apache.spark.serializer.Serializer
import org.apache.spark.{Aggregator, Partitioner, Partition}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

// is there a way to not extend shuffledrdd, just reuse the original shuffledrdd?
class HBaseShuffledRDD[K, V, C](
    @transient var prevRdd: RDD[_ <: Product2[K, V]],
    partitoner: Partitioner) extends ShuffledRDD(prevRdd, partitoner){

  private var serializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  private var hbPartitions: Seq[HBasePartition] = Seq.empty

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBasePartition].server.map {
      identity[String]
    }.toSeq
  }

  def setHbasePartitions(hbPartitions: Seq[HBasePartition]): HBaseShuffledRDD[K, V, C]  = {
    this.hbPartitions = hbPartitions
    this
  }

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  override def setSerializer(serializer: Serializer): HBaseShuffledRDD[K, V, C] = {
    this.serializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  override def setKeyOrdering(keyOrdering: Ordering[K]): HBaseShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  // why here use override get error?
  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): HBaseShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  override def setMapSideCombine(mapSideCombine: Boolean): HBaseShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  override def getPartitions: Array[Partition] = {
    if (hbPartitions.isEmpty) {
      Array.tabulate[Partition](partitoner.numPartitions)(i => new HBasePartition(i))
    } else {
      hbPartitions.toArray
    }
  }
}
