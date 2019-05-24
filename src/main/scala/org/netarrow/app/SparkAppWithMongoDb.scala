package org.netarrow.app

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

//TODO: consider setting MongoDB settings on MongoSpark instead
trait SparkAppWithMongoDb extends SparkApp {
  override protected def createSparkSession(appName: String)(implicit config: Config): SparkSession = {
    val mongodbServer = config.getString("mongodb.server")
    createSparkSessionBuilder(appName)
      //TODO: how to connect to two dbs or two collections?
      .config("spark.mongodb.input.uri", s"mongodb://$mongodbServer/test2.user")
      .config("spark.mongodb.output.uri", s"mongodb://$mongodbServer/test2.user")
      .getOrCreate()
  }
}
