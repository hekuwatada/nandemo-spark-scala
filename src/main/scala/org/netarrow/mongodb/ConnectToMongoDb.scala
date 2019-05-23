package org.netarrow.mongodb

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.netarrow.model.User

object ConnectToMongoDb extends App {

  //@see https://docs.mongodb.com/spark-connector/master/scala-api/
  /* Create the SparkSession.
 * If config arguments are passed from the command line using --conf,
 * parse args for the values to set.
 */

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test2.user")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test2.user")
    .getOrCreate()

  import spark.implicits._

  val users: Dataset[User] = spark.sparkContext
    .parallelize(Seq(User("bear", 10), User("rabbit", 5)))
    .toDS()

  MongoSpark.save(users)

  val ds: Dataset[User] = MongoSpark
    .load(spark).as[User]
    .filter(_.age < 10)

  val ret = ds.count()

  println(ds.first())

  println(ret)

  spark.stop()
}