package org.apache.spark.sql.hbase

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Row, SchemaRDD}

/**
 * TestingSchemaRDD
 * Created by sboesch on 10/6/14.
 */
class TestingSchemaRDD(@transient sqlContext: HBaseSQLContext,
    @transient baseLogicalPlan: LogicalPlan)
    extends SchemaRDD(sqlContext, baseLogicalPlan) {
  @transient val logger = Logger.getLogger(getClass.getName)

  /** A private method for tests, to look at the contents of each partition */
  override private[spark] def collectPartitions(): Array[Array[Row]] = {
    sparkContext.runJob(this, (iter: Iterator[Row]) => iter.toArray, partitions.map{_.index},
      allowLocal=true)
  }

}
