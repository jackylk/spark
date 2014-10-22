package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

/**
 * Created by mengbo on 10/22/14.
 */
private[hbase] case class HBaseRelation(configuration: Configuration, context: HBaseSQLContext,
                                   catalogTable: HBaseCatalogTable) extends LeafNode {

}
