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

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

class HBaseSQLParser extends SqlParser {
  protected val CREATE = Keyword("CREATE")
  protected val DROP = Keyword("DROP")
  protected val ALTER = Keyword("ALTER")
  protected val EXISTS = Keyword("EXISTS")
  protected val MAPPED = Keyword("MAPPED")
  protected val ADD = Keyword("ADD")

  override def apply(sql: String) = super.apply(sql)

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
    CREATE ~> TABLE ~> opt(IF ~ NOT ~ EXISTS ^^^ true) ~
      ident ~ ("(" ~> tableCols <~ ")") ~ (MAPPED ~> BY ~> "(" ~> ident <~ ",") ~
      colFamilies <~ ")" <~ opt(";") ^^ {
      case ine ~ tn ~ tc ~ htn ~ cf =>
        println("\nin Create")
        println(ine)
        println(tn)
        println(tc)
        println(htn)
        println(cf)
        null
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
    } | ALTER ~> TABLE ~> ident ~ ADD ~ tableCol ~ (MAPPED ~> BY ~> "(" ~> colFamily <~ ")") ^^ {
      case tn ~ op ~ tc ~ cf => {
        println("\nin Alter")
        println(tn)
        println(op)
        println(tc)
        println(cf)
        null
      }
    }

  protected lazy val tableCol: Parser[Expression] =
    expression ~ (expression | STRING) ^^ {
      case e1 ~ e2 => Alias(e1, e2.toString)()
    }

  protected lazy val tableCols: Parser[Seq[Expression]] = repsep(tableCol, ",")

  protected lazy val colFamily: Parser[Expression] = expression ^^ { case e => e}

  protected lazy val colFamilies: Parser[Seq[Expression]] = repsep(colFamily, ",")

}
