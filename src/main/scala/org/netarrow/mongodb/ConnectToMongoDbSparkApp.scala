package org.netarrow.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bson.Document
import org.netarrow.model.User

//@see https://docs.mongodb.com/spark-connector/master/scala-api/
/* Create the SparkSession.
* If config arguments are passed from the command line using --conf,
* parse args for the values to set.
*/

object WriteToMongoDb$App extends App with SparkAppWithMongoDb {
  val appName = "WriteToMongoDb"

  run { ss: SparkSession =>
    import ss.implicits._

    val users: Dataset[User] = ss.sparkContext
      .parallelize(Seq(User("bear", 10), User("rabbit", 5)))
      .toDS()

    MongoSpark.save(users)
  }

}

object ReadFromMongoDb$App extends App with SparkAppWithMongoDb {

  //@see https://docs.mongodb.com/spark-connector/master/scala/aggregation/

  val appName = "ReadFromMongoDb"

  run { ss: SparkSession =>
    import ss.implicits._

    /**
      * Data set and filter
      */
    println(">>> Load as Dataset then filter")
    val ds: Dataset[User] = MongoSpark
      .load(ss).as[User]
      .filter(_.age < 10)

    val ret = ds.count()

    println(ret)
    println(ds.first())

    /**
      * MongoRDD and filter documents
      */
    println(">>> Load as MongoRDD then filter - no easy object codec")
    // shortcut of below is:
    // val rdd: MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
    import com.mongodb.spark._
    val mongoRdd: MongoRDD[Document] = ss.sparkContext.loadFromMongoDB()
    val rddDoc: RDD[Document] = mongoRdd
      .filter(doc => doc.getInteger("age") < 10) // toDS[T] does not exist at this point

    println(rddDoc.count())

    /**
      * Use Spark Context to load to MongoRDD
      */
    println(">>> Load as MongoRDD then convert to Dataset")
    val rdd: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
    val docs: Dataset[User] = rdd.toDS[User]() // toDS[T] works without filter

    println(docs.first())

    /**
      * Filter on MongoRDD and convert to Dataset
      */
    println(">>> Load as MongoRDD then use MongoDB aggregation pipeline - works with toDS")
    val filteredMongoRdd: MongoRDD[Document] = MongoSpark.load(ss.sparkContext)
      .withPipeline(List( // 2)
        Document.parse("""{ $match: { age: { $gte: 5 } } }""")
      ))
    val filteredUsers: Dataset[User] = filteredMongoRdd.toDS[User]

    println(filteredUsers.first())
  }
}

trait SparkAppWithMongoDb {
  def appName: String

  def run(jobBlock: SparkSession => Unit): Unit = {
    val ss = createSparkSession(appName)

    try {
      jobBlock(ss)
    } finally {
      ss.stop()
    }
  }

  private def createSparkSession(appName: String): SparkSession =
    SparkSession.builder()
      .master("local")
      .appName(appName)
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test2.user")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test2.user")
      .getOrCreate()
}