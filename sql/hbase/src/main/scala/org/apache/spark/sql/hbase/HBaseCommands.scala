package org.apache.spark.sql.hbase

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{Command, LeafNode}


case class CreateTableCommand(tableName: String,
                              tableCols: Seq[(String, String)],
                              hbaseTable: String,
                              keys: Seq[String],
                              otherCols: Seq[(String, String)])(@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.createHbaseTable(tableName, tableCols, hbaseTable, keys, otherCols)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}