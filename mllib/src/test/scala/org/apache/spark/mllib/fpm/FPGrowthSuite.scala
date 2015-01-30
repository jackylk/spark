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
package org.apache.spark.mllib.fpm

import org.scalatest.FunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class FPGrowthSuite extends FunSuite with MLlibTestSparkContext {

  test("test FPGrowth algorithm")
  {
    val arr = FPGrowthSuite.createFIMDataSet4()
    val dataSet = sc.parallelize(arr)
    val rdd = dataSet.map(line => line.split(" "))

    val algorithm = new FPGrowth()
    algorithm.setMinSupport(0.5)

    FPGrowthSuite.printFrequentPattern(algorithm.run(rdd).frequentPattern)
    //assert(algorithm.run(rdd).frequentPattern.length == 2)

        /*
    algorithm.setMinSupport(0.8)
    assert(algorithm.run(rdd).frequentPattern.length == 2)
    algorithm.setMinSupport(0.7)
    assert(algorithm.run(rdd).frequentPattern.length == 2)
    algorithm.setMinSupport(0.6)
    assert(algorithm.run(rdd).frequentPattern.length == 2)
    algorithm.setMinSupport(0.5)
    assert(algorithm.run(rdd).frequentPattern.length == 3)
    algorithm.setMinSupport(0.4)
    assert(algorithm.run(rdd).frequentPattern.length == 3)
    algorithm.setMinSupport(0.3)
    assert(algorithm.run(rdd).frequentPattern.length == 3)
    algorithm.setMinSupport(0.2)
    assert(algorithm.run(rdd).frequentPattern.length == 3)
    algorithm.setMinSupport(0.1)
    assert(algorithm.run(rdd).frequentPattern.length == 3)
    FPGrowthSuite.printFrequentPattern(algorithm.run(rdd).frequentPattern) */
  }
  /*
  test("test FPGrowth algorithm")
  {
    val arr = FPGrowthSuite.createFIMDataSet()

    assert(arr.length == 6)
    val dataSet = sc.parallelize(arr)
    assert(dataSet.count() == 6)
    val rdd = dataSet.map(line => line.split(" "))
    assert(rdd.count() == 6)

    val algorithm = new FPGrowth()
    algorithm.setMinSupport(0.9)
    assert(algorithm.run(rdd).frequentPattern.length == 0)
    algorithm.setMinSupport(0.8)
    assert(algorithm.run(rdd).frequentPattern.length == 1)
    algorithm.setMinSupport(0.7)
    assert(algorithm.run(rdd).frequentPattern.length == 1)
    algorithm.setMinSupport(0.6)
    assert(algorithm.run(rdd).frequentPattern.length == 2)
    algorithm.setMinSupport(0.5)
    assert(algorithm.run(rdd).frequentPattern.length == 18)
    algorithm.setMinSupport(0.4)
    assert(algorithm.run(rdd).frequentPattern.length == 18)
    algorithm.setMinSupport(0.3)
    assert(algorithm.run(rdd).frequentPattern.length == 54)
    algorithm.setMinSupport(0.2)
    assert(algorithm.run(rdd).frequentPattern.length == 54)
    algorithm.setMinSupport(0.1)
    assert(algorithm.run(rdd).frequentPattern.length == 625)
  }
  */
/*
  test("test FIM with FPTree")
  {
    val data = FPGrowthSuite.createFIMDataSet()
    val data1 = FPGrowthSuite.createFIMDataSet1()
    val data2 = FPGrowthSuite.createFIMDataSet2()
    val data3 = FPGrowthSuite.createFIMDataSet3()

    val rdd = sc.parallelize(data)
    val l1Map = rdd.flatMap(v => v.split(" ")).map(v => (v, 1)).reduceByKey(_+_).collect().toMap
    println(l1Map)

    val tree = new FPTree()
    for (transaction <- data){
      val transactionsort = transaction.split(" ").map(item => (item, l1Map(item))).sortWith(_._2 > _._2).map(x => x._1)
      transactionsort.foreach(x => print(x + " "))
      tree.add(transactionsort)
      println()
    }
    FPGrowthSuite.printTree(tree)

    val tree1 = new FPTree()
    for (transaction <- data1){
      tree1.add(
        transaction.split(" ").map(item => (item,l1Map(item))).sortBy(_._2).map(x => x._1)
      )
    }
    FPGrowthSuite.printTree(tree1)
    val tree2 = new FPTree()
    for (transaction <- data2){
      tree2.add(
        transaction.split(" ").map(item => (item,l1Map(item))).sortBy(_._2).map(x => x._1)
      )
    }
    FPGrowthSuite.printTree(tree2)
    val tree3 = new FPTree()
    for (transaction <- data3){
      tree3.add(
        transaction.split(" ").map(item => (item,l1Map(item))).sortBy(_._2).map(x => x._1)
      )
    }
    FPGrowthSuite.printTree(tree3)
    val mergeTree = tree1.merge(tree2).merge(tree3)
    FPGrowthSuite.printTree(mergeTree)

    assert(mergeTree.root.children.keySet.size == tree.root.children.keySet.size)
  }
*/
}

object FPGrowthSuite
{
  /**
   * Create test data set
   */
  def createFIMDataSet():Array[String] =
  {
    val arr = Array[String](
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
    arr
  }

  def createFIMDataSet4():Array[String] =
  {
    val arr = Array[String](
      "r z h j p",
      "z y x w v u t s",
      "z",
      "r x n o s",
      "y r x z q t p",
      "y z x e q s t m")
    arr
  }

  def createFIMDataSet1():Array[String] =
  {
    Array[String](
      "a b c",
      "a b")
  }

  def createFIMDataSet2():Array[String] =
  {
    Array[String](
      "s x o n r",
      "x z y m t s q e")
  }

  def createFIMDataSet3():Array[String] =
  {
    Array[String](
      "z",
      "x z y r q t p")
  }

  def printTree(tree: FPTree) = printTreeRoot(tree.root, 0)

  private def printTreeRoot(tree: FPTreeNode, level: Int): Unit = {
    printNode(tree, level)
    if (tree.isLeaf) return
    val it = tree.children.iterator
    while (it.hasNext) {
      val child = it.next()
      printTreeRoot(child._2, level + 1)
    }
  }

  private def printNode(node: FPTreeNode, level: Int) = {
    for (i <- 0 to level) {
      print("\t")
    }
    println(node.item + " " + node.count)
  }

  def printFrequentPattern(pattern: Array[(Array[String], Long)]) = {
    for (a <- pattern) {
      a._1.foreach(x => print(x + " "))
      print(a._2)
      println
    }
  }
}
