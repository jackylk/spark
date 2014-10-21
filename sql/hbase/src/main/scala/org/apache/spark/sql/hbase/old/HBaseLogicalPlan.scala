package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.plans.logical.Command

case class CreateHBaseTablePlan(tableName: String,
                                nameSpace: String,
                                hbaseTable: String,
                                colsSeq: Seq[String],
                                keyCols: Seq[(String, String)],
                                nonKeyCols: Seq[(String, String, String, String)]
                                 ) extends Command

case class DropTablePlan(tableName: String) extends Command
