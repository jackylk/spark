package org.apache.spark.sql.hbase

import org.apache.spark.Logging
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SkelTest extends FunSuite with BeforeAndAfterAll with Logging {

  test("foo") {
    foo
  }
  def foo() = {
    println("bar")
  }

}

object SkelTest {

  def main(args: Array[String]) = {
    new SkelTest().foo
  }
}
