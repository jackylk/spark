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

import java.lang.reflect.Method

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.SqlLexical
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

class HBaseSQLParser extends SqlParser {
  protected val CREATE = Keyword("CREATE")
  protected val DROP = Keyword("DROP")
  protected val ALTER = Keyword("ALTER")
  protected val EXISTS = Keyword("EXISTS")
  protected val MAPPED = Keyword("MAPPED")
  protected val ADD = Keyword("ADD")
  protected val KEYS = Keyword("KEYS")
  protected val COLS = Keyword("COLS")

  protected val newReservedWords:Seq[String] =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(newReservedWords)

  override protected lazy val query: Parser[LogicalPlan] = (
    select * (
      UNION ~ ALL ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2)} |
        INTERSECT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2)} |
        EXCEPT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)} |
        UNION ~ opt(DISTINCT) ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2))}
      )
      | insert | cache | create | drop | alter
    )

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> TABLE ~> ident ~
      ("(" ~> tableCols <~ ")") ~
      (MAPPED ~> BY ~> "(" ~> ident <~ ",") ~
      (KEYS ~> "=" ~> "[" ~> keys <~ "]" <~ ",") ~
      (COLS ~> "=" ~> "[" ~> expressions <~ "]" <~ ")") <~ opt(";") ^^ {
      case tableName ~ tableCols ~ htn ~ keys ~ otherCols =>
        val otherColsSeq:Seq[(String, String)] =
          otherCols.map{case EqualTo(e1, e2) => (e1.toString.substring(1), e2.toString.substring(1))}
        CreateTablePlan(tableName, tableCols, htn, keys, otherColsSeq)
    }

  protected lazy val drop: Parser[LogicalPlan] =
    DROP ~> TABLE ~> ident <~ opt(";") ^^ {
      case tn =>
        println("\nin Drop")
        println(tn)
        null
    }

  protected lazy val alter: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~ DROP ~ ident <~ opt(";") ^^ {
      case tn ~ op ~ col => {
        println("\nin Alter")
        println(tn)
        println(op)
        println(col)
        null
      }
    } | ALTER ~> TABLE ~> ident ~ ADD ~ tableCol ~ (MAPPED ~> BY ~> "(" ~> expressions <~ ")") ^^ {
      case tn ~ op ~ tc ~ cf => {
        println("\nin Alter")
        println(tn)
        println(op)
        println(tc)
        println(cf)
        null
      }
    }

  protected lazy val tableCol: Parser[(String, String)] =
    ident ~ (ident | STRING) ^^ {
      case e1 ~ e2 => (e1, e2)
    }

  protected lazy val tableCols: Parser[Seq[(String, String)]] = repsep(tableCol, ",")

  protected lazy val keys: Parser[Seq[String]] = repsep(ident, ",")

  protected lazy val expressions: Parser[Seq[Expression]] = repsep(expression, ",")

}

case class CreateTablePlan( tableName: String,
                            tableCols: Seq[(String, String)],
                            hbaseTable: String,
                            keys: Seq[String],
                            otherCols: Seq[(String, String)]) extends Command 
