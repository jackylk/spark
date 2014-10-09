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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import DataTypeUtils.compare
import org.apache.spark.sql.DataType

/**
 * RelationalOperator
 * Created by sboesch on 9/24/14.
 */
sealed trait HRelationalOperator {
  def toHBase: CompareOp
  def cmp(col1: Any, col2: Any): Boolean
  def cmp(col1: HBaseRawType, dataType1: DataType,
          col2: HBaseRawType, dataType2: DataType): Boolean
}

case object LT extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.LESS
  }

  def cmp(col1: Any, col2: Any): Boolean = compare(col1, col2) < 0

  override def cmp(col1: HBaseRawType, dataType1: DataType,
                   col2: HBaseRawType, dataType2: DataType): Boolean
    = compare(col1,dataType1, col2, dataType2) < 0
}

case object LTE extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.LESS_OR_EQUAL
  }
  def cmp(col1: Any, col2: Any): Boolean = compare(col1, col2) <= 0

  override def cmp(col1: HBaseRawType, dataType1: DataType,
                   col2: HBaseRawType, dataType2: DataType): Boolean
    = compare(col1,dataType1, col2, dataType2) <= 0
}

case object EQ extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.EQUAL
  }
  def cmp(col1: Any, col2: Any): Boolean = compare(col1, col2) == 0

  override def cmp(col1: HBaseRawType, dataType1: DataType,
                   col2: HBaseRawType, dataType2: DataType): Boolean
    =  compare(col1,dataType1, col2, dataType2) == 0
}

case object GTE extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.GREATER_OR_EQUAL
  }
  def cmp(col1: Any, col2: Any): Boolean = compare(col1, col2) >= 0

  override def cmp(col1: HBaseRawType, dataType1: DataType,
                   col2: HBaseRawType, dataType2: DataType): Boolean
    = compare(col1,dataType1, col2, dataType2) >= 0
}

case object GT extends HRelationalOperator {
  override def toHBase: CompareOp = {
    CompareOp.GREATER
  }
  def cmp(col1: Any, col2: Any): Boolean = compare(col1, col2) > 0

  override def cmp(col1: HBaseRawType, dataType1: DataType,
                   col2: HBaseRawType, dataType2: DataType): Boolean
    = compare(col1,dataType1, col2, dataType2) > 0
}
