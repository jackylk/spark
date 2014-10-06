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

import org.apache.hadoop.hbase.client.Put
import org.apache.log4j.Logger
import org.apache.spark.sql.StructType
import org.apache.spark.sql.catalyst.expressions.{Row, Attribute}
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
                                         externalResource : Option[HBaseExternalResource])
  extends LeafNode {

  self: Product =>

  @transient val logger = Logger.getLogger(getClass.getName)

  @transient lazy val tableName = catalogTable.hbaseTableName.tableName

  val partitions : Seq[HBasePartition] = catalogTable.partitions

  lazy val partitionKeys: Seq[Attribute] = catalogTable.rowKey.columns.asAttributes

  lazy val attributes = catalogTable.columns.asAttributes

  lazy val colFamilies = catalogTable.colFamilies

  @transient lazy val rowKeyParser = catalogTable.rowKeyParser

  def rowToHBasePut(schema: StructType, row: Row): Put = {
    val ctab = catalogTable
    val rkey = rowKeyParser.createKeyFromCatalystRow(schema, ctab.rowKey.columns, row)
    val p = new Put(rkey)
    CatalystToHBase.catalystRowToHBaseRawVals(schema, row, ctab.columns).zip(ctab.columns.columns)
      .map{ case (raw, col) => p.add(s2b(col.family), s2b(col.qualifier), raw)
    }
    p
  }

//  // TODO: Set up the external Resource
  def getExternalResource : Option[HBaseExternalResource] = externalResource

  override def output: Seq[Attribute] = attributes ++ partitionKeys


}
