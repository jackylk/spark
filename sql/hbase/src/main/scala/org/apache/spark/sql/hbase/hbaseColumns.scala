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

import java.util

import org.apache.spark.sql.DataType
import org.apache.spark.sql.catalyst.expressions._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{StringType, LongType, IntegerType}

case class ColumnName(var family: Option[String], qualifier: String) {
  if (family.isDefined && family.get==null) {
    family = None
  }

  override def toString = fullName

  def fullName = if (family.isDefined) {
    s"$family:$qualifier"
  } else {
    s":$qualifier"
  }

//  override def equals(other: Any) = {
//    if (!other.isInstanceOf[ColumnName]) {
//      false
//    }
//    val cother = other.asInstanceOf[ColumnName]
//    family == cother.family && qualifier == cother.qualifier
//  }
}

object ColumnName {
  def apply(compoundStr: String) = {
    val toks = compoundStr.split(":").toList
    if (toks.size == 2) {
      new ColumnName(Some(toks(0)), toks(1))
    } else {
      new ColumnName(None, toks(0))
    }
    //    toks match {
    //      case fam :: qual => new ColumnName(Some(toks(0)), toks(1))
    //      case qual => new ColumnName(None, toks(1))
    //    }
  }
}

/**
 * Initially we support initially predicates of the form
 * col RELOP literal
 * OR
 * literal RELOP col
 *
 * The ColumnOrLiteral allows us to represent that restrictions
 */
sealed trait ColumnOrLiteral

case class HColumn(colName: ColumnName) extends ColumnOrLiteral

case class HLiteral(litval: Any) extends ColumnOrLiteral

//case class ColumnVal(colName: HColumn, colVal: Option[Any] = None)

case class ColumnPredicate(left: ColumnOrLiteral, right: ColumnOrLiteral,
                           op: HRelationalOperator = EQ)

// TODO: how is the (ColumnFam,ColumnName) stored in attribute?

object ColumnPredicate {
  val EmptyColumnPredicate = ColumnPredicate(null, null, EQ)

  def catalystToHBase(predicate: BinaryComparison) = {
    def fromExpression(expr: Expression) = expr match {
      case lit: Literal => HLiteral(lit.eval(null))
      case attrib: AttributeReference => HColumn(ColumnName(attrib.name))
      case Cast(child, dataType : DataType) => dataType match {
        case IntegerType => HLiteral(child.eval(null).toString.toInt)
        case LongType => HLiteral(child.eval(null).toString.toLong)
        case StringType => HLiteral(child.eval(null).toString)
        case _ => throw new UnsupportedOperationException(
          s"CAST not yet supported for dataType ${dataType}")
      }

      case _ => throw new UnsupportedOperationException(
        s"fromExpression did not understand ${expr.toString}")
    }

    def catalystClassToRelOp(catClass: BinaryComparison) = catClass match {
      case LessThan(_, _) => LT
      case LessThanOrEqual(_, _) => LTE
      case EqualTo(_, _) => EQ
      case GreaterThanOrEqual(_, _) => GTE
      case GreaterThan(_, _) => GT
      case _ => throw new UnsupportedOperationException(catClass.getClass.getName)
    }
    val leftColOrLit = fromExpression(predicate.left)
    val rightColOrLit = fromExpression(predicate.right)
    ColumnPredicate(leftColOrLit, rightColOrLit, catalystClassToRelOp(predicate))
  }
}

