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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.BooleanType
import org.apache.spark.sql.catalyst.expressions.{Row, BindReferences, Expression, Attribute}
import org.apache.spark.sql.execution.LeafNode

/**
 * HBaseTableScan
 * Created by sboesch on 9/2/14.
 */
case class HBaseTableScan(
                          attributes: Seq[Attribute],
                          relation: HBaseRelation,
                          predicates : Option[Expression],
//                          partitionPruningPred: Option[Expression])(
                          @transient val context: HBaseSQLContext)
  extends LeafNode {
//  override lazy val logger = Logger.getLogger(getClass.getName)

//  // Bind all partition key attribute references in the partition pruning predicate for later
//  // evaluation.
//  private[this] val boundPruningPred = partitionPruningPred.map { pred =>
//    require(
//      pred.dataType == BooleanType,
//      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")
//
//    BindReferences.bindReference(pred, relation.)
//  }

//  private[this] val hbaseReader = new HBaseReader(attributes, relation, context)
//  override def execute() = {
//    HBase
//  }


  /**
   * Runs this query returning the result as an RDD.
   */
  override def execute(): RDD[Row] = ???

  override def output = attributes

}
