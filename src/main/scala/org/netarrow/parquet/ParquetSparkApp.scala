package org.netarrow.parquet

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.netarrow.app.SparkApp
import org.netarrow.model.User

//@see https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

object WriteAsParquetSparkApp extends App with SparkApp {
  val appName = "WriteAsParquetSparkApp"
  implicit val config: Config = ConfigFactory.load()

  run { ss: SparkSession =>
    import ss.implicits._

    val users = Seq(User("okame", 45), User("hyottoko", 19), User("okina", 37))
    val ds: Dataset[User] = ss.sparkContext.parallelize(users).toDS()

    ds.write.parquet("user.parquet")
  }
}

object ReadFromParquetSparkApp extends App with SparkApp {
  val appName = "ReadFromParquetSparkApp"
  implicit val config: Config = ConfigFactory.load()

  run { ss: SparkSession =>
    import ss.implicits._
    val schema = Encoders.product[User].schema

    val ds: Dataset[User] = ss.read
      .schema(schema)
      .parquet("user.parquet")
      .as[User]

    println(ds.first())
  }
}

