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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.hbase.HBaseCatalog._

/**
 * HBaseRelation
 *
 * Created by stephen.boesch@huawei.com on 9/8/14
 */


private[hbase] case class HBaseRelation (
//                                         @transient configuration: Configuration,
//                                         @transient hbaseContext: HBaseSQLContext,
//                                         htable: HTableInterface,
                                         catalogTable: HBaseCatalogTable,
                                         externalResource : ExternalResource)
  extends LeafNode {

  self: Product =>

  // TODO: Set up the external Resource
  def getExternalResource : HBaseExternalResource = ???

  //  val namespace = catalogTable.tableName.getNamespace

  val tableName = catalogTable.tableName

  val partitions : Seq[HBasePartition] = catalogTable.partitions
  val logger = Logger.getLogger(getClass.getName)

  val partitionKeys: Seq[Attribute] = catalogTable.rowKey.columns.asAttributes

  val attributes = catalogTable.columns.asAttributes

  val colFamilies = catalogTable.colFamilies.seq

  override def output: Seq[Attribute] = attributes ++ partitionKeys


}
