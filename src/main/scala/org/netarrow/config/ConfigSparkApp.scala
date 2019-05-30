package org.netarrow.config

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.netarrow.app.SparkApp

object ConfigSparkApp extends App with SparkApp {

  val appName = "ConfigSparkApp"

  implicit val config = ConfigFactory.load()

  run { ss: SparkSession =>
    println(ss.conf.getAll)
  }
}
