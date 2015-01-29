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
    /*
    val thisTree = this.root
    val compTree = tree.root
    while ( compTree.children != null){
      if (thisTree.children == null){

      }else{
        val thisKeyIterator = thisTree.children.keysIterator
        val compKeyIterator = compTree.children.keys
        if (compKeyIterator(0) != null) {
          val compKey = compKeyIterator.next()
          var k = 0
          while (thisKeyIterator.hasNext) {
            val thisKey = thisKeyIterator.next()
            if (thisKey.equals(compKey)) {
              thisTree.children(thisKey).count = thisTree.children(thisKey).count
              k = k + 1
            }
          }
          if (k == 0){

          }
        }
      }
    }
    */

    val left: Seq = Seq[(String, Long)]()
    val right: Seq = Seq()
    var curr = root
    while (curr != null) {

    }

    this
  }

  def extract(threshold: Double, prefix: String): Iterator[(Array[String], Long)] = {

  }

}

class FPTreeNode(val item: String, var count: Int) {
  val parent: FPTreeNode = null
  val children: Map[String, FPTreeNode] = Map[String, FPTreeNode]()
}
