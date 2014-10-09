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

import org.apache.commons.el.RelationalOperator
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Attribute, Expression, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LeafNode

/**
 * HBaseTableScan
 * Created by sboesch on 9/2/14.
 */
case class HBaseSQLTableScan(
                              ignoredAttributes: Seq[Attribute],
                              attributes: Seq[Attribute],
                              relation: HBaseRelation,
                              projList: Seq[ColumnName],
                              predicates: Option[Expression],
                              partitionPruningPred: Option[Expression],
                              rowKeyPredicates: Option[Seq[ColumnPredicate]],
                              externalResource: Option[HBaseExternalResource],
                              plan: LogicalPlan)
                            (@transient context: HBaseSQLContext)
  extends LeafNode {

  /**
   * Runs this query returning the result as an RDD.
   */
  override def execute(): RDD[Row] = {

    var colFilters : Option[FilterList] = None
    if (HBaseStrategies.PushDownPredicates) {
      // Now process the projection predicates
      var invalidPreds = false
      var colPredicates: Option[Seq[ColumnPredicate]] = if (!predicates.isEmpty) {
        val bs = predicates.map {
          case pp: BinaryComparison =>
            ColumnPredicate.catalystToHBase(pp)
          //        case s =>
          //          log.info(s"ColPreds: Only BinaryComparison operators supported ${s.toString}")
          //          invalidPreds = true
          //          null.asInstanceOf[Option[Seq[ColumnPredicate]]]
        }.filter(_ != null).asInstanceOf[Seq[ColumnPredicate]]
        Some(bs)
      } else {
        None
      }
      if (invalidPreds) {
        colPredicates = None
      }

//      val colNames = relation.catalogTable.rowKey.columns.columns.
//        map{ c => ColumnName(Some(c.family), c.qualifier)
//      }
//

      // TODO: Do column pruning based on only the required colFamilies
      val filters = new HBaseSQLFilters(relation.colFamilies,
        relation.catalogTable.rowKey.columns.columns,
        rowKeyPredicates, colPredicates
        )
      val colFilters = filters.createColumnFilters

    // TODO(sboesch): Perform Partition pruning based on the rowKeyPredicates

    }
    new HBaseSQLReaderRDD(relation.catalogTable.hbaseTableName,
      externalResource,
      relation,
      projList,
      relation.partitions,
      relation.colFamilies,
      colFilters,
      /* rowKeyPredicates, colPredicates */
      context
      /*attributes,*/
    )
  }

  override def output = attributes

}
