package pagerank

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SmartPR {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.cores.max", "15").set("spark.ui.port", "9898")
    
    val sc = new SparkContext(args(0), "SmartPR", conf)
    val ranks = pagerank(sc, args(1), args(2).toInt, 9)
    //    ranks.collect.foreach(print)
  }

  def pagerank(sc: SparkContext, edgesFile: String, numIter: Int): RDD[(Int, Double)] = {
    val edges = sc.textFile(edgesFile, 10).map(line => {
      val arr = line.split("\\s")
      (arr(0).toInt, arr(1).toInt)
    }).cache
    pagerank(sc, edges, numIter)
  }

  def pagerank(sc: SparkContext, edgesFile: String, numVertex: Int, numIter: Int): RDD[(Int, Double)] = {
    val edges = sc.textFile(edgesFile, 10).map(line => {
      val arr = line.split("\\s")
      (arr(0).toInt, arr(1).toInt)
    }).cache
    pagerank(sc, edges, numVertex, numIter)
  }

  def pagerank(sc: SparkContext, edges: RDD[(Int, Int)], numIter: Int): RDD[(Int, Double)] = {
    val number = Math.max(edges.groupBy(_._1).keys.max, edges.groupBy(_._2).keys.max)
    pagerank(sc, edges, number, numIter)
  }

  def pagerank(sc: SparkContext, edges: RDD[(Int, Int)], numVertex: Int, numIter: Int): RDD[(Int, Double)] = {
    val sizex = new Array[Double](numVertex)
    val prx = new Array[Double](numVertex)
    edges.groupBy(_._1).mapValues(_.size).collect.foreach(v => {
      sizex(v._1 - 1) = v._2
    })

    val metrix = edges.groupBy(_._2).mapValues(_.map(_._1)).cache
    var ranks = metrix.mapValues(v => 1.0)
    for (iter <- 0 until numIter) {
      ranks.collect.foreach(v => {
        prx(v._1 - 1) = v._2 / sizex(v._1 - 1)
      })
      ranks = metrix.map(row => {
        val pr = row._2.map(t => prx(t - 1)).reduce(_ + _)
        (row._1, 0.15 + 0.85 * pr)
      })
    }
    return ranks
  }
}