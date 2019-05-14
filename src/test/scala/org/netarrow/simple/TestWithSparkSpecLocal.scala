package org.netarrow.simple

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.netarrow.model.User
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

/**
  * This test is to illustrate running unit tests with local Spark
  */
class TestWithSparkSpecLocal extends FunSpec with Matchers with SparkLocal {
  val appName = "TestWithSparkLocalSpec"

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

    it("creates pair RDD with simple case class with serializable values") {
      withSparkContext { sc: SparkContext =>
        val users: Seq[User] = Seq(User("name1", 10), User("name2", 10), User("name3", 35))
        val rdd: RDD[User] = sc.parallelize(users)
        val pairRdd: RDD[(Int, String)] = rdd.map(u => (u.age, u.name))
        val groupedPairRdd: RDD[(Int, Iterable[String])] = pairRdd.groupByKey()
        val groupedUsers: Array[(Int, Iterable[String])] = groupedPairRdd.collect()
        groupedUsers should contain theSameElementsAs Array((10, Seq("name1", "name2")), (35, Seq("name3")))
      }
    }
  }

  describe("Dataset") {
    it("creates Dataset from source data in memory") {
      withSparkSession { ss: SparkSession =>
        import ss.implicits._

        val ds: Dataset[Int] = Seq(1, 2, 3, 4, 5, 2, 3, 4).toDS()
        //        ds.explain(true) // action
        val distinctDs: Dataset[Int] = ds.dropDuplicates()
        val collectedData: Array[Int] = distinctDs.collect()
        collectedData should contain theSameElementsAs Array(1, 2, 3, 4, 5)
      }
    }

    it("creates Dataset from RDD") {
      withSparkSession { ss: SparkSession =>
        import ss.implicits._
        val sc = ss.sparkContext

        val ds: Dataset[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5)).toDS()
        //TODO: implement with mapPartition
        ds.reduce(_ + _) shouldBe 15
      }
    }
  }
}
