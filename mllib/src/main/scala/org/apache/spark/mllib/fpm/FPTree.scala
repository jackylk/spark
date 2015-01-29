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

import scala.collection.mutable.ArrayBuffer

class FPTree {

  private val root: FPTreeNode = new FPTreeNode(null, 0)

  def add(transaction: Array[String]): this.type = {
    var index = 0
    val size = transaction.size
    var curr: FPTreeNode = root
    while (index < size) {
      val node = curr.children(transaction(index))
      if (node != null) {
        node.count = node.count + 1
        curr = node
      } else {
        curr.children + ((transaction(index), new FPTreeNode(transaction(index), 1)))
        curr = curr.children(transaction(index))
      }
      index = index + 1
    }

    // TODO: in oder to further reduce the amount of data for shuffle,
    // remove the same pattern which has the same hash number
    this
  }

  def merge(tree: FPTree): this.type = {
    // simply merge all children of the input tree to this.root
    if (tree.root.children != null) {
      val it = tree.root.children.iterator
      while (it.hasNext) {
        this.root.children + it.next
      }
    }
    this
  }

  def extract(threshold: Double, prefix: String): Array[(Array[String],Long)] = {
    val condPattBase = unfold(this)
    combine(condPattBase, threshold)
  }

  private def combine(
    condPattBase: (String, Iterable[Array[String]]),
    minCount: Double): Array[(Array[String],Long)] = {

  }

  private def unfold(tree: FPTree): (String, Iterable[Array[String]]) = {

  }
}

class FPTreeNode(val item: String, var count: Int) {
  val parent: FPTreeNode = null
  val children: Map[String, FPTreeNode] = Map[String, FPTreeNode]()
}
