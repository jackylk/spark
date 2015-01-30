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

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class FPTreeSuite extends FunSuite with MLlibTestSparkContext {
  /*
  test("add transaction to tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("b"))
    FPGrowthSuite.printTree(tree)
    assert(tree.root.children.size == 2)
    assert(tree.root.children.contains("a"))
    assert(tree.root.children("a").item.equals("a"))
    assert(tree.root.children("a").count == 2)
    assert(tree.root.children.contains("b"))
    assert(tree.root.children("b").item.equals("b"))
    assert(tree.root.children("b").count == 1)
    val n = tree.root.children("a")
    // TODO
  }

  test("merge tree") {
    val tree1 = new FPTree
    tree1.add(Array[String]("a", "b", "c"))
    tree1.add(Array[String]("a", "b", "y"))
    tree1.add(Array[String]("b"))

    FPGrowthSuite.printTree(tree1)

    val tree2 = new FPTree
    tree2.add(Array[String]("a", "b"))
    tree2.add(Array[String]("a", "b", "c"))
    tree2.add(Array[String]("a", "b", "c", "d"))
    tree2.add(Array[String]("a", "x"))
    tree2.add(Array[String]("a", "x", "y"))
    tree2.add(Array[String]("c", "n"))
    tree2.add(Array[String]("c", "m"))

    FPGrowthSuite.printTree(tree2)

    val tree3 = tree1.merge(tree2)
    FPGrowthSuite.printTree(tree3)
  }

  test("expand tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("a", "b"))
    tree.add(Array[String]("a"))
    tree.add(Array[String]("b"))
    tree.add(Array[String]("b", "n"))

    FPGrowthSuite.printTree(tree)
    val buffer = tree.expandFPTree(tree)
    for (a <- buffer) {
      a.foreach(x => print(x + " "))
      println
    }
  }
  */

  test("mine tree") {
    val tree = new FPTree
    tree.add(Array[String]("a", "b", "c"))
    tree.add(Array[String]("a", "b", "y"))
    tree.add(Array[String]("a", "b"))
    tree.add(Array[String]("a"))
    tree.add(Array[String]("b"))
    tree.add(Array[String]("b", "n"))

    FPGrowthSuite.printTree(tree)
    val buffer = tree.mine(3.0, "t")

    for (a <- buffer) {
      a._1.foreach(x => print(x + " "))
      print(a._2)
      println
    }
  }
}
