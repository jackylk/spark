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
package org.apache.spark.mllib.fim

import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

/**
 * An example runner for decision trees and random forests. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 *
 * Note: This script treats all features as real-valued (not categorical).
 *       To include categorical features, modify categoricalFeaturesInfo.
 */
class AprioriSuite extends FunSuite with LocalSparkContext {

  test("test FIM with Apriori")
  {
    val arr = AprioriSuite.createFIMDataSet()
    assert(arr.length === 6)
    val dataSet = sc.parallelize(arr)
    assert(dataSet.count() == 6)
    val rdd = dataSet.map(line => line.split(" "))
    assert(rdd.count() == 6)

    for (i <- 1 to 9){
      println(s"frequent item set with support ${i/10d}")
      AprioriArray.apriori(rdd, i/10d, sc).foreach(x => print("(" + x._1 + "), "))
      println()
    }

    assert(AprioriArray.apriori(rdd,0.9,sc).length == 0)

    assert(AprioriArray.apriori(rdd,0.8,sc).length == 1)

    assert(AprioriArray.apriori(rdd,0.7,sc).length == 1)

    assert(AprioriArray.apriori(rdd,0.6,sc).length == 2)

    assert(AprioriArray.apriori(rdd,0.5,sc).length == 18)

    assert(AprioriArray.apriori(rdd,0.4,sc).length == 18)

    assert(AprioriArray.apriori(rdd,0.3,sc).length == 54)

    assert(AprioriArray.apriori(rdd,0.2,sc).length == 54)

    assert(AprioriArray.apriori(rdd,0.1,sc).length == 625)

  }
}

/**
 * create dataset
 */
object AprioriSuite
{
  /**
   * create dataset using Practical Machine Learning Book data
   * @return dataset
   */
  def createFIMDataSet():Array[String] =
  {
    val arr = Array[String](
    "1 3 4",
    "2 3 5",
    "1 2 3 5",
    "2 5")

    /*
    val arr = Array[String](
      "r z h j p",
      "z y x w v u t s",
      "z",
      "r x n o s",
      "y r x z q t p",
      "y z x e q s t m")
      */
    return arr
  }
}

