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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{Command, LeafNode}

case class CreateHBaseTableCommand(tableName: String,
                                   nameSpace: String,
                                   hbaseTable: String,
                                   colsSeq: Seq[String],
                                   keyCols: Seq[(String, String)],
                                   nonKeyCols: Seq[(String, String, String, String)])
                                  (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    val catalog = context.catalog

    val keyColumns = keyCols.map { case (name, typeOfData) =>
      KeyColumn(name, catalog.getDataType(typeOfData.toLowerCase))
    }
    val nonKeyColumns = nonKeyCols.map {
      case (name, typeOfData, family, qualifier) =>
        NonKeyColumn(name, catalog.getDataType(typeOfData), family, qualifier)
    }

//    catalog.createTable(nameSpace, tableName, hbaseTable, colSeq, keyColumns, nonKeyColumns)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class DropHbaseTableCommand(tableName: String)
                                (@transient context: HBaseSQLContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult = {
    context.catalog.deleteTable(tableName)
    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}
