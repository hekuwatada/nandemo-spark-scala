package org.netarrow.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.bson.Document
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

  /**
    * Data set and filter
    */
  val ds: Dataset[User] = MongoSpark
    .load(spark).as[User]
    .filter(_.age < 10)

//  val ret = ds.count()
//
//  println(ds.first())


  ////// WIP //////

  /**
    * MongoRDD and filter documents
    */
  //@see https://docs.mongodb.com/spark-connector/master/scala/aggregation/

  // shortcut of below is:
  // val rdd: MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
  import com.mongodb.spark._
  val mongoRdd: MongoRDD[Document] =
    spark.sparkContext.loadFromMongoDB()
  val rddDoc: RDD[Document] = mongoRdd
    .filter(doc => doc.getInteger("age") < 10) // toDS[T] does not work at this point

  /**
    * Use Spark Context to load to RDD
    */
  val rdd: MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
  val docs: Dataset[User] = rdd.toDS[User]() // toDS[T] works without filter

  println(docs.first())


  spark.stop()
}