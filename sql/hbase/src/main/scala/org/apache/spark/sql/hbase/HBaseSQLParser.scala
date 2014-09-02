package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.SqlLexical
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

class HBaseSQLParser extends SqlParser{
  protected val CREATE = Keyword("CREATE")

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> opt(DISTINCT)  <~ opt(";") ^^ {
      //to-do
      null
    }
}
