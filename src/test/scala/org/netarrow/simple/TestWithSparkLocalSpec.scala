package org.netarrow.simple

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FunSpec, Matchers}

/**
  * This test is to illustrate running unit tests with local Spark
  */
class TestWithSparkLocalSpec extends FunSpec with Matchers {
  val appName = "TestWithSparkLocalSpec"

  private def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      //NOTE: Local in-process mode
      // driver is used for execution
      // uses as many threads as the number of processors available to JVM
      //@see https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-local.html
      .master("local[*]")
      .getOrCreate()
  }

  private def withSparkSession(testBlock: (SparkSession) => Unit): Unit = {
    val ss = createSparkSession(appName)
    try {
      testBlock(ss)
    } finally {
      ss.sparkContext.stop()
    }
  }

  private def withSparkContext(testBlock: (SparkContext) => Unit): Unit =
    withSparkSession((ss: SparkSession) => testBlock(ss.sparkContext))


  describe("Testing with local Spark") {
    it("starts a local Spark") {
      withSparkSession { ss: SparkSession =>
        ss.sparkContext.isLocal shouldBe true
      }
    }
  }

  describe("RDD") {
    it("creates RDD") {
      withSparkContext { sc: SparkContext =>
        val numbers: Seq[Int] = Range(1, 1000000)
        val rdd: RDD[Int] = sc.parallelize(numbers)
        val filteredRdd: RDD[String] = rdd.filter(_ % 2 != 0).map(_.toString)
        filteredRdd.count() shouldBe 500000
      }
    }
  }
}
