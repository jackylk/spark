package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}
import org.apache.log4j.Logger

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

/**
 * CreateTableAndLoadData
 *
 */
trait CreateTableAndLoadData {

  private val logger = Logger.getLogger(getClass.getName)
  val DefaultStagingTableName = "StageTable"
  val DefaultTableName = "TestTable"
  val DefaultHbaseStagingTabName = "stageTab"
  val DefaultHbaseTabName = "testTab"
  val DefaultHbaseColFamiles = Seq("cf1", "cf2")
  val DefaultLoadFile = "./sql/hbase/src/test/resources/testTable.csv"

  var AvoidRowkeyBug = false

  def createTableAndLoadData(hbc: HBaseSQLContext) = {
    createTables(hbc)
    loadData(hbc)
  }

  def createNativeHbaseTable(hbc: HBaseSQLContext, tableName: String, families: Seq[String]) = {
    val hbaseAdmin = hbc.catalog.hBaseAdmin
    val hdesc = new HTableDescriptor(tableName)
    families.foreach { f => hdesc.addFamily(new HColumnDescriptor(f))}
    hbaseAdmin.createTable(hdesc)
  }

  def createTables(hbc: HBaseSQLContext, stagingTableName: String = DefaultStagingTableName, tableName: String = DefaultTableName) = {
    // this need to local test with hbase, so here to ignore this

    val hbaseAdmin = hbc.catalog.hBaseAdmin
    createNativeHbaseTable(hbc, DefaultHbaseStagingTabName, DefaultHbaseColFamiles)
    createNativeHbaseTable(hbc, DefaultHbaseTabName, DefaultHbaseColFamiles)

    val (stagingSql, tabSql) = if (AvoidRowkeyBug) {
      ( s"""CREATE TABLE $stagingTableName(strcol STRING, bytecol String, shortcol String, intcol String,
            longcol string, floatcol string, doublecol string,
            PRIMARY KEY(strcol, intcol,doublecol))
            MAPPED BY ($DefaultHbaseStagingTabName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
        .stripMargin
        ,
        s"""CREATE TABLE $tableName(strcol STRING, bytecol BYTE, shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE,
            PRIMARY KEY(strcol, intcol,doublecol))
            MAPPED BY ($DefaultHbaseTabName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
          .stripMargin
        )
    } else {
      ( s"""CREATE TABLE $stagingTableName(strcol STRING, bytecol String, shortcol String, intcol String,
            longcol string, floatcol string, doublecol string, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($DefaultHbaseStagingTabName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
        .stripMargin
        ,
        s"""CREATE TABLE $tableName(strcol STRING, bytecol BYTE, shortcol SHORT, intcol INTEGER,
            longcol LONG, floatcol FLOAT, doublecol DOUBLE, PRIMARY KEY(doublecol, strcol, intcol))
            MAPPED BY ($DefaultHbaseTabName, COLS=[bytecol=cf1.hbytecol,
            shortcol=cf1.hshortcol, longcol=cf2.hlongcol, floatcol=cf2.hfloatcol])"""
          .stripMargin
        )
    }
    var executeSql1 = hbc.executeSql(stagingSql)
    executeSql1.toRdd.collect().foreach(println)

    logger.debug(s"Created table $tableName: " +
      s"isTableAvailable= ${hbaseAdmin.isTableAvailable(s2b(DefaultHbaseStagingTabName))}" +
      s" tableDescriptor= ${hbaseAdmin.getTableDescriptor(s2b(DefaultHbaseStagingTabName))}")


    executeSql1 = hbc.executeSql(tabSql)
    executeSql1.toRdd.collect().foreach(println)

  }

  def loadData(hbc: HBaseSQLContext, tableName: String = DefaultTableName,
               loadFile: String = DefaultLoadFile) = {
    // then load data into table
    val hbaseAdmin = hbc.catalog.hBaseAdmin
    val loadSql = s"LOAD DATA LOCAL INPATH '$loadFile' INTO TABLE $tableName"
    val result3 = hbc.executeSql(loadSql).toRdd.collect()
    val insertSql = s"""insert into $tableName select cast(strcol as string),
    cast(bytecol as tinyint), cast(shortcol as smallint), cast(intcol as int),
    cast (longcol as bigint), cast(floatcol as float), cast(doublecol as double)
    from $DefaultHbaseStagingTabName"""
  }

  def s2b(s: String) = Bytes.toBytes(s)

}
