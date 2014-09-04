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
  protected val DROP = Keyword("DROP")
  protected val ALTER = Keyword("ALTER")
  protected val EXISTS = Keyword("EXISTS")
  protected val MAPPED = Keyword("MAPPED")

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> TABLE ~> opt(IF ~ NOT ~ EXISTS ^^^ true) ~ ident ~ ("(" ~> tableCols <~ ")") ~ (MAPPED ~> BY ~> "(" ~> ident <~ ",") ~ colFamilies <~ ")" <~ opt(";") ^^ {
      case i ~ t ~ r ~ a ~ b=>
        println("\nin Create")
        println(i)
        println(t)
        println(r)
        println(a)
        println(b)
        null
    }

  protected lazy val drop: Parser[LogicalPlan] =
    DROP ~> TABLE ~> ident <~ opt(";") ^^ {
      case t =>
        println("\nin Drop")
        println(t)
        null
    }

  protected lazy val colFamily: Parser[Expression] = expression ^^ {case e => e}

  protected lazy val colFamilies: Parser[Seq[Expression]] = repsep(colFamily, ",")

  protected lazy val tableCol: Parser[Expression] =
    expression ~ (expression | STRING)  ^^ {
      case e1 ~ e2 => Alias(e1, e2.toString)()
    }

  protected lazy val tableCols: Parser[Seq[Expression]] = repsep(tableCol, ",")

  protected lazy val alter: Parser[LogicalPlan] =
    ALTER ~> opt(OVERWRITE) ~ inTo ~ select <~ opt(";") ^^ {
      case o ~ r ~ s =>
        val overwrite: Boolean = o.getOrElse("") == "OVERWRITE"
        InsertIntoTable(r, Map[String, Option[String]](), s, overwrite)
    }

}
