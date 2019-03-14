package org.netarrow.simple

import org.apache.spark.sql.SparkSession
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

  def withSparkLocal(testBlock: (SparkSession) => Unit): Unit = {
    val ss = createSparkSession(appName)
    try {
      testBlock(ss)
    } finally {
      ss.sparkContext.stop()
    }
  }
  
  describe("Testing with local Spark") {
    it("starts a local Spark") { withSparkLocal { ss: SparkSession =>
        ss.sparkContext.isLocal shouldBe true
      }
    }
  }
}
