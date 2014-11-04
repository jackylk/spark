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

package org.apache.spark.sql.hbase.execution

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, HFileOutputFormat}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{LeafNode, UnaryNode, SparkPlan}
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.HBasePartitioner._

import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * The HBase table scan operator.
 */
@DeveloperApi
case class HBaseSQLTableScan(
    relation: HBaseRelation,
    output: Seq[Attribute],
    rowKeyPredicate: Option[Expression],
    valuePredicate: Option[Expression],
    partitionPredicate: Option[Expression],
    coProcessorPlan: Option[SparkPlan])
    (@transient context: HBaseSQLContext)
  extends LeafNode {

  override def execute(): RDD[Row] = {
    new HBaseSQLReaderRDD(
      relation,
      output,
      rowKeyPredicate, // TODO:convert to column pruning preds
      valuePredicate,
      partitionPredicate, // PartitionPred : Option[Expression]
      None, // coprocSubPlan: SparkPlan
      context
    )
  }
}

@DeveloperApi
case class InsertIntoHBaseTable(
    relation: HBaseRelation,
    child: SparkPlan)
    (@transient hbContext: HBaseSQLContext)
  extends UnaryNode {

  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)
    // YZ: to be implemented using sc.runJob() => SparkContext needed here
    childRdd
  }

  override def output = child.output
}

@DeveloperApi
case class BulkLoadIntoTable(path: String, relation: HBaseRelation, isLocal: Boolean)(
  @transient hbContext: HBaseSQLContext) extends LeafNode {

  val conf = hbContext.sc.hadoopConfiguration

  val job = new Job(hbContext.sc.hadoopConfiguration)

  val hadoopReader = if (isLocal) {
    val fs = FileSystem.getLocal(conf)
    val pathString = fs.pathToFile(new Path(path)).getCanonicalPath
    new HadoopReader(hbContext.sparkContext, job, pathString)(relation.allColumns)
  } else {
    new HadoopReader(hbContext.sparkContext, job, path)(relation.allColumns)
  }

  // tmp path for storing HFile
  val tmpPath = Util.getTempFilePath(conf, relation.tableName)

  private[hbase] def makeBulkLoadRDD(splitKeys: Array[SparkImmutableBytesWritable]) = {
    val ordering = HBasePartitioner.orderingRowKey
      .asInstanceOf[Ordering[SparkImmutableBytesWritable]]
    val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    val partitioner = new HBasePartitioner(rdd)(splitKeys)
    val shuffled =
      new ShuffledRDD[SparkImmutableBytesWritable, SparkPut, SparkPut](rdd, partitioner)
        .setKeyOrdering(ordering)
    val bulkLoadRDD = shuffled.mapPartitions { iter =>
    // the rdd now already sort by key, to sort by value
      val map = new java.util.TreeSet[KeyValue](KeyValue.COMPARATOR)
      var preKV: (SparkImmutableBytesWritable, SparkPut) = null
      var nowKV: (SparkImmutableBytesWritable, SparkPut) = null
      val ret = new ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()
      if(iter.hasNext) {
        preKV = iter.next()
        var cellsIter = preKV._2.toPut().getFamilyCellMap.values().iterator()
        while(cellsIter.hasNext()) {
          cellsIter.next().foreach { cell =>
            val kv = KeyValueUtil.ensureKeyValue(cell)
            map.add(kv)
          }
        }
        while(iter.hasNext) {
          nowKV = iter.next()
          if(0 == (nowKV._1 compareTo preKV._1)) {
            cellsIter = nowKV._2.toPut().getFamilyCellMap.values().iterator()
            while(cellsIter.hasNext()) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          } else {
            ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable(), _))
            preKV = nowKV
            map.clear()
            cellsIter = preKV._2.toPut().getFamilyCellMap.values().iterator()
            while(cellsIter.hasNext()) {
              cellsIter.next().foreach { cell =>
                val kv = KeyValueUtil.ensureKeyValue(cell)
                map.add(kv)
              }
            }
          }
        }
        ret ++= map.iterator().map((preKV._1.toImmutableBytesWritable(), _))
        map.clear()
        ret.iterator
      } else {
        Iterator.empty
      }
    }
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat])
    job.getConfiguration.set("mapred.output.dir", tmpPath)
    bulkLoadRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  override def execute() = {
    val splitKeys = relation.getRegionStartKeys().toArray
    makeBulkLoadRDD(splitKeys)
    val hbaseConf = HBaseConfiguration.create
    val tablePath = new Path(tmpPath)
    val load = new LoadIncrementalHFiles(hbaseConf)
    load.doBulkLoad(tablePath, relation.htable)
    hbContext.sc.parallelize(Seq.empty[Row], 1)
  }

  override def output = Nil

}
